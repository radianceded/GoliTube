<#
.SYNOPSIS
  把一轮矩阵的采样、档位边界和每档 final.json 合并成一张分析表。

.DESCRIPTION
  只使用档位正式运行窗口内的采样：开始后 5 秒到结束前 2 秒，避开建连、预热尾部和收尾。
  计数器类指标用窗口首尾差值计算速率；Kafka 积压取窗口最大值，避免只看结束时已经排空的值。
#>
param(
  [Parameter(Mandatory = $true)][string]$RunDir
)

$ErrorActionPreference = "Stop"
$RunDir = (Resolve-Path $RunDir).Path
$samplesPath = Join-Path $RunDir "samples.csv"
$gearsPath = Join-Path $RunDir "gears.csv"
if (-not (Test-Path $samplesPath)) { throw "缺少 $samplesPath" }
if (-not (Test-Path $gearsPath)) { throw "缺少 $gearsPath" }

function To-Number {
  param($Value)
  if ($null -eq $Value -or "$Value" -eq "") { return $null }
  return [double]$Value
}

function Measure-Stats {
  param([object[]]$Values)
  $numbers = @($Values | Where-Object { $null -ne $_ })
  if ($numbers.Count -eq 0) {
    return [pscustomobject]@{ Avg = $null; Max = $null; Min = $null }
  }
  $sum = ($numbers | Measure-Object -Sum).Sum
  return [pscustomobject]@{
    Avg = $sum / $numbers.Count
    Max = ($numbers | Measure-Object -Maximum).Maximum
    Min = ($numbers | Measure-Object -Minimum).Minimum
  }
}

$samples = Import-Csv $samplesPath
foreach ($sample in $samples) { $sample | Add-Member -NotePropertyName parsed_time -NotePropertyValue ([datetime]::Parse($sample.time)) -Force }
$gears = Import-Csv $gearsPath
$results = @()

foreach ($gear in $gears) {
  $started = [datetime]::Parse($gear.started).AddSeconds(5)
  $ended = [datetime]::Parse($gear.ended).AddSeconds(-2)
  $window = @($samples | Where-Object { $_.parsed_time -ge $started -and $_.parsed_time -le $ended })
  if ($window.Count -eq 0) { $window = @($samples | Where-Object { $_.parsed_time -ge $started }) }

  $first = $window | Select-Object -First 1
  $last = $window | Select-Object -Last 1
  $seconds = if ($first -and $last) { ($last.parsed_time - $first.parsed_time).TotalSeconds } else { 0 }
  if ($seconds -le 0) { $seconds = 1 }

  $serverCpu = Measure-Stats ($window | ForEach-Object { To-Number $_.server_cpu_pct })
  $loadCpu = Measure-Stats ($window | ForEach-Object { To-Number $_.load_cpu_pct })
  $serverRss = Measure-Stats ($window | ForEach-Object { To-Number $_.server_rss_mb })
  $loadRss = Measure-Stats ($window | ForEach-Object { To-Number $_.load_rss_mb })
  $goroutines = Measure-Stats ($window | ForEach-Object { To-Number $_.server_goroutines })
  $heap = Measure-Stats ($window | ForEach-Object { To-Number $_.server_heap_mb })
  $queued = Measure-Stats ($window | ForEach-Object { To-Number $_.server_queued })
  $redisOps = Measure-Stats ($window | ForEach-Object { To-Number $_.redis_ops_per_s })
  $redisPublish = Measure-Stats ($window | ForEach-Object { To-Number $_.redis_publish_us })
  $redisCpu = Measure-Stats ($window | ForEach-Object { To-Number $_.redis_cpu_pct })
  $kafkaCpu = Measure-Stats ($window | ForEach-Object { To-Number $_.kafka_cpu_pct })
  $mysqlCpu = Measure-Stats ($window | ForEach-Object { To-Number $_.mysql_cpu_pct })
  $lag = Measure-Stats ($window | ForEach-Object { To-Number $_.kafka_lag })

  $deliveriesStart = To-Number $first.server_deliveries
  $deliveriesEnd = To-Number $last.server_deliveries
  $serverDeliveryRate = if ($null -ne $deliveriesStart -and $null -ne $deliveriesEnd) { ($deliveriesEnd - $deliveriesStart) / $seconds } else { $null }

  $slowStart = To-Number $first.server_slow_evictions
  $slowEnd = To-Number $last.server_slow_evictions
  $slowEvictions = if ($null -ne $slowStart -and $null -ne $slowEnd) { $slowEnd - $slowStart } else { $null }

  $rowsStart = To-Number $first.mysql_rows_inserted
  $rowsEnd = To-Number $last.mysql_rows_inserted
  $writesPerSecond = if ($null -ne $rowsStart -and $null -ne $rowsEnd) { ($rowsEnd - $rowsStart) / $seconds } else { $null }

  $txCountStart = To-Number $first.mysql_tx_count
  $txCountEnd = To-Number $last.mysql_tx_count
  $txSumStart = To-Number $first.mysql_tx_sum_us
  $txSumEnd = To-Number $last.mysql_tx_sum_us
  $commitsPerSecond = if ($null -ne $txCountStart -and $null -ne $txCountEnd) { ($txCountEnd - $txCountStart) / $seconds } else { $null }
  $transactionUS = if ($null -ne $txCountStart -and $null -ne $txCountEnd -and ($txCountEnd - $txCountStart) -gt 0) {
    ($txSumEnd - $txSumStart) / ($txCountEnd - $txCountStart)
  } else { $null }

  $finalPath = Join-Path $RunDir (Join-Path $gear.gear "final.json")
  $front = $null
  if (Test-Path $finalPath) {
    $final = Get-Content $finalPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $front = $final.totals
  }

  $p99 = if ($front -and $front.latency.count -gt 0) { $front.latency.p99_ms } else { $null }
  $missingRate = if ($front) { $front.missing_rate } else { $null }
  $disconnects = if ($front) { $front.disconnects } else { $null }
  $rateLimited = if ($front) { $front.rate_limited } else { $null }

  # 验收目标：P99 ≤ 200ms、缺失 ≤ 0.1%、无持续断连、Kafka 积压不增长。
  # 积压看本档前后差值：前面的档位可能已经留下积压，不能把存量算到当前档头上。
  $lagBefore = [long]$gear.kafka_lag_before
  $lagEnd = [long]$gear.kafka_lag_after
  $lagDelta = $lagEnd - $lagBefore
  $verdict = "通过"
  $reasons = @()
  if ($null -ne $p99 -and $p99 -gt 200) { $reasons += "P99>200ms" }
  if ($null -ne $missingRate -and $missingRate -gt 0.001) { $reasons += "缺失>0.1%" }
  if ($null -ne $disconnects -and $disconnects -gt 0) { $reasons += "有断连" }
  if ($null -ne $rateLimited -and $rateLimited -gt 0) { $reasons += "被限流" }
  if ($lagDelta -gt 100) { $reasons += "Kafka积压增长" }
  if ($reasons.Count -gt 0) { $verdict = "不通过：" + ($reasons -join "、") }

  $results += [pscustomobject]@{
    gear = $gear.gear
    connections = $gear.connections
    rooms = $gear.rooms
    rate_per_room = $gear.rate_per_room
    sent_per_s = if ($front) { [math]::Round($front.send_per_second, 1) } else { $null }
    delivered_per_s = if ($front) { [math]::Round($front.receive_per_second, 1) } else { $null }
    server_delivery_per_s = if ($null -ne $serverDeliveryRate) { [math]::Round($serverDeliveryRate, 1) } else { $null }
    p50_ms = if ($front) { $front.latency.p50_ms } else { $null }
    p95_ms = if ($front) { $front.latency.p95_ms } else { $null }
    p99_ms = $p99
    missing_rate_pct = if ($null -ne $missingRate) { [math]::Round($missingRate * 100, 4) } else { $null }
    duplicates = if ($front) { $front.duplicates } else { $null }
    disconnects = $disconnects
    rate_limited = $rateLimited
    server_cpu_avg_pct = if ($null -ne $serverCpu.Avg) { [math]::Round($serverCpu.Avg, 2) } else { $null }
    server_cpu_max_pct = if ($null -ne $serverCpu.Max) { [math]::Round($serverCpu.Max, 2) } else { $null }
    server_rss_max_mb = if ($null -ne $serverRss.Max) { [math]::Round($serverRss.Max, 1) } else { $null }
    goroutines_max = if ($null -ne $goroutines.Max) { [int]$goroutines.Max } else { $null }
    heap_max_mb = if ($null -ne $heap.Max) { [math]::Round($heap.Max, 1) } else { $null }
    queued_max = if ($null -ne $queued.Max) { [int]$queued.Max } else { $null }
    slow_evictions = $slowEvictions
    load_cpu_avg_pct = if ($null -ne $loadCpu.Avg) { [math]::Round($loadCpu.Avg, 2) } else { $null }
    load_rss_max_mb = if ($null -ne $loadRss.Max) { [math]::Round($loadRss.Max, 1) } else { $null }
    redis_cpu_avg_pct = if ($null -ne $redisCpu.Avg) { [math]::Round($redisCpu.Avg, 2) } else { $null }
    redis_ops_avg = if ($null -ne $redisOps.Avg) { [math]::Round($redisOps.Avg, 0) } else { $null }
    redis_publish_us_max = if ($null -ne $redisPublish.Max) { [math]::Round($redisPublish.Max, 2) } else { $null }
    kafka_cpu_avg_pct = if ($null -ne $kafkaCpu.Avg) { [math]::Round($kafkaCpu.Avg, 2) } else { $null }
    kafka_lag_max = if ($null -ne $lag.Max) { [long]$lag.Max } else { $null }
    kafka_lag_end = $lagEnd
    kafka_lag_delta = $lagDelta
    mysql_cpu_avg_pct = if ($null -ne $mysqlCpu.Avg) { [math]::Round($mysqlCpu.Avg, 2) } else { $null }
    mysql_writes_per_s = if ($null -ne $writesPerSecond) { [math]::Round($writesPerSecond, 1) } else { $null }
    mysql_commits_per_s = if ($null -ne $commitsPerSecond) { [math]::Round($commitsPerSecond, 1) } else { $null }
    mysql_tx_avg_us = if ($null -ne $transactionUS) { [math]::Round($transactionUS, 1) } else { $null }
    verdict = $verdict
  }
}

$analysisPath = Join-Path $RunDir "analysis.csv"
$results | Export-Csv -Path $analysisPath -NoTypeInformation -Encoding utf8

$results | Format-Table gear, connections, sent_per_s, delivered_per_s, p99_ms, missing_rate_pct,
  server_cpu_avg_pct, server_rss_max_mb, goroutines_max, queued_max, slow_evictions,
  kafka_lag_max, kafka_lag_delta, mysql_writes_per_s, mysql_tx_avg_us, verdict -AutoSize

Write-Host ""
Write-Host "分析表：$analysisPath"
