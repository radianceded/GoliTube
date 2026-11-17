<#
.SYNOPSIS
  依次运行空闲连接、多房间广播、热点房间三组档位，并持续采样资源与积压。

.DESCRIPTION
  运行前需要已有 server（GOLITUBE_METRICS=1）和 persist 进程，脚本用 -ServerPid 指定 server，
  以免把 persist 的 CPU 算进服务端。每档开始前等待 Kafka 积压清零，结束后再次等待并记录。
  采样与档位边界分别写入 samples.csv 和 gears.csv，逐档结果在各个子目录的 final.json。
#>
param(
  [string]$Tag = ("matrix-" + (Get-Date -Format "yyyyMMdd-HHmmss")),
  [string]$OutRoot = "build\loadtest-results",
  [string]$Exe = "build\loadtest.exe",
  [string]$Base = "http://127.0.0.1:18081",
  [string]$MySqlDSN = "golitube:golitube-local@tcp(127.0.0.1:23306)/golitube_loadtest",
  [string]$RedisAddr = "127.0.0.1:16379",
  [Parameter(Mandatory = $true)][int]$ServerPid,
  [int]$SampleIntervalSeconds = 10,
  [string]$Only = "",
  [switch]$SkipEnvironment
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $projectRoot

# 原生进程继承的是进程工作目录而不是 PowerShell 位置，所有传给子进程的路径都用绝对路径。
if (-not [System.IO.Path]::IsPathRooted($OutRoot)) { $OutRoot = Join-Path $projectRoot $OutRoot }
if (-not [System.IO.Path]::IsPathRooted($Exe)) { $Exe = Join-Path $projectRoot $Exe }
$runDir = Join-Path $OutRoot $Tag
New-Item -ItemType Directory -Force $runDir | Out-Null
$stopFile = Join-Path $runDir "collector.stop"
if (Test-Path $stopFile) { Remove-Item $stopFile }

# 只跑一次，因此每档不做三次重复；时长缩短为 60～90 秒，用于定位拐点而不是出具正式容量结论。
$gears = @(
  [pscustomobject]@{ Name = "idle-500";   Conn = 500;  Rooms = 20;  Rate = 0;  Warmup = 20; Duration = 60; ConnectRate = 250 },
  [pscustomobject]@{ Name = "idle-1000";  Conn = 1000; Rooms = 20;  Rate = 0;  Warmup = 20; Duration = 60; ConnectRate = 250 },
  [pscustomobject]@{ Name = "idle-2000";  Conn = 2000; Rooms = 20;  Rate = 0;  Warmup = 20; Duration = 60; ConnectRate = 250 },
  [pscustomobject]@{ Name = "idle-5000";  Conn = 5000; Rooms = 20;  Rate = 0;  Warmup = 20; Duration = 60; ConnectRate = 300 },
  [pscustomobject]@{ Name = "mr-10x1";    Conn = 100;  Rooms = 10;  Rate = 1;  Warmup = 20; Duration = 90; ConnectRate = 100 },
  [pscustomobject]@{ Name = "mr-50x5";    Conn = 500;  Rooms = 50;  Rate = 5;  Warmup = 20; Duration = 90; ConnectRate = 100 },
  [pscustomobject]@{ Name = "mr-100x10";  Conn = 1000; Rooms = 100; Rate = 10; Warmup = 20; Duration = 90; ConnectRate = 100 },
  [pscustomobject]@{ Name = "mr-200x10";  Conn = 2000; Rooms = 200; Rate = 10; Warmup = 20; Duration = 90; ConnectRate = 100 },
  [pscustomobject]@{ Name = "mr-300x10";  Conn = 3000; Rooms = 300; Rate = 10; Warmup = 20; Duration = 90; ConnectRate = 150 },
  [pscustomobject]@{ Name = "hs-100x50";  Conn = 100;  Rooms = 1;   Rate = 50; Warmup = 20; Duration = 90; ConnectRate = 100 },
  [pscustomobject]@{ Name = "hs-500x100"; Conn = 500;  Rooms = 1;   Rate = 100; Warmup = 20; Duration = 90; ConnectRate = 100 },
  [pscustomobject]@{ Name = "hs-1000x200"; Conn = 1000; Rooms = 1;  Rate = 200; Warmup = 20; Duration = 90; ConnectRate = 150 },
  [pscustomobject]@{ Name = "stability-1000"; Conn = 1000; Rooms = 100; Rate = 10; Warmup = 60; Duration = 1800; ConnectRate = 100 }
)
if ($Only) {
  $wanted = $Only -split ","
  $gears = $gears | Where-Object { $wanted -contains $_.Name }
}

function Get-KafkaLag {
  $previous = $ErrorActionPreference
  $ErrorActionPreference = "Continue"
  try {
    $output = & docker exec golitube-loadtest-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh `
      --bootstrap-server localhost:9092 --describe --group golitube-persistence-v1 2>$null
  } finally {
    $ErrorActionPreference = $previous
  }
  $lag = 0
  foreach ($line in $output) {
    $parts = $line -split '\s+'
    if ($parts.Count -ge 6 -and $parts[5] -match '^\d+$') { $lag += [long]$parts[5] }
  }
  return [long]$lag
}

function Wait-Drain {
  param([int]$TimeoutSeconds = 120, [string]$Label = "")
  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  while ((Get-Date) -lt $deadline) {
    $lag = Get-KafkaLag
    if ($lag -eq 0) { return $true }
    Start-Sleep -Seconds 2
  }
  Write-Host "  警告：$Label 在 $TimeoutSeconds 秒内未清空 Kafka 积压，当前 $lag"
  return $false
}

if (-not $SkipEnvironment) {
  & (Join-Path $PSScriptRoot "collect-environment.ps1") -OutDir $runDir -ProjectRoot $projectRoot
}

$collector = Start-Job -FilePath (Join-Path $PSScriptRoot "collect.ps1") -ArgumentList @(
  $runDir, $ServerPid, $SampleIntervalSeconds, $stopFile
)
Start-Sleep -Seconds 4
if ($collector.State -ne "Running") {
  Receive-Job $collector
  throw "采集器没有正常运行"
}

$gearRows = [System.Collections.Generic.List[object]]::new()
try {
  foreach ($gear in $gears) {
    Write-Host ""
    Write-Host ("=== {0}：连接 {1}，房间 {2}，每房间 {3}/s ===" -f $gear.Name, $gear.Conn, $gear.Rooms, $gear.Rate)
    Write-Host "等待 Kafka 积压清零..."
    $lagBefore = Get-KafkaLag
    if ($lagBefore -gt 0) { Wait-Drain -TimeoutSeconds 120 -Label "档位前积压" | Out-Null }
    $lagBefore = Get-KafkaLag
    Start-Sleep -Seconds 3

    $outputDir = Join-Path $runDir $gear.Name
    $logPath = Join-Path $runDir ("{0}.log" -f $gear.Name)
    $arguments = @(
      "-base", $Base,
      "-connections", $gear.Conn,
      "-rooms", $gear.Rooms,
      "-rate", $gear.Rate,
      "-warmup", ("{0}s" -f $gear.Warmup),
      "-duration", ("{0}s" -f $gear.Duration),
      "-connect-rate", $gear.ConnectRate,
      "-mysql-dsn", $MySqlDSN,
      "-redis-addr", $RedisAddr,
      "-run-id", ("{0}-{1}" -f $Tag, $gear.Name),
      "-out", $outputDir,
      "-summary-interval", "10s"
    )
    $started = Get-Date
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
      & $Exe @arguments 2>&1 | Tee-Object -FilePath $logPath
      $exitCode = $LASTEXITCODE
    } finally {
      $ErrorActionPreference = $previous
    }
    $ended = Get-Date
    $lagAfter = Get-KafkaLag
    $drained = Wait-Drain -TimeoutSeconds 120 -Label "$($gear.Name) 结束后积压"

    $gearRows.Add([pscustomobject]@{
      gear = $gear.Name
      connections = $gear.Conn
      rooms = $gear.Rooms
      rate_per_room = $gear.Rate
      started = $started.ToString("o")
      ended = $ended.ToString("o")
      seconds = [math]::Round(($ended - $started).TotalSeconds, 1)
      kafka_lag_before = $lagBefore
      kafka_lag_after = $lagAfter
      drained = $drained
      exit_code = $exitCode
      output_dir = $outputDir
    })
  }
}
finally {
  New-Item -ItemType File -Force $stopFile | Out-Null
  $null = Wait-Job $collector -Timeout 60
  $collectorErrors = Receive-Job $collector
  if ($collectorErrors) { $collectorErrors | Select-Object -First 20 | Out-String | Write-Host }
  Remove-Job $collector -Force
  $gearRows | Export-Csv -Path (Join-Path $runDir "gears.csv") -NoTypeInformation -Encoding utf8
}

Write-Host ""
Write-Host "档位汇总："
foreach ($row in $gearRows) {
  $finalPath = Join-Path $row.output_dir "final.json"
  if (Test-Path $finalPath) {
    $final = Get-Content $finalPath -Raw -Encoding utf8 | ConvertFrom-Json
    $p99 = if ($final.totals.latency.count -gt 0) { "{0:N1}" -f $final.totals.latency.p99_ms } else { "-" }
    Write-Host ("{0,-14} 连接={1,5} 发送={2,7:N1}/s 投递={3,8:N1}/s 缺失={4,7:N3}% P99={5,7}ms 断连={6} 限流={7} 积压后={8}" -f `
        $row.gear, $final.totals.connections, $final.totals.send_per_second, $final.totals.receive_per_second,
      ($final.totals.missing_rate * 100), $p99, $final.totals.disconnects, $final.totals.rate_limited, $row.kafka_lag_after)
  }
}
Write-Host ""
Write-Host "结果目录：$runDir"
