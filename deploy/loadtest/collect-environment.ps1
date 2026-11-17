<#
.SYNOPSIS
  记录压测机器的硬件、系统、Go 版本、提交 SHA 和中间件部署参数。
#>
param(
  [Parameter(Mandatory = $true)][string]$OutDir,
  [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
)

$ErrorActionPreference = "Stop"
New-Item -ItemType Directory -Force $OutDir | Out-Null

function Try-Value {
  param([scriptblock]$Block, $Fallback = $null)
  try { return & $Block } catch { return $Fallback }
}

$cpu = Get-CimInstance Win32_Processor | Select-Object -First 1
$system = Get-CimInstance Win32_ComputerSystem
$os = Get-CimInstance Win32_OperatingSystem
$goVersion = (& go version) -join " "
$gitSha = (& git -C $ProjectRoot rev-parse HEAD) -join ""
$gitBranch = (& git -C $ProjectRoot rev-parse --abbrev-ref HEAD) -join ""
$gitStatus = (& git -C $ProjectRoot status --porcelain) -join "`n"

$containers = @()
foreach ($name in @("golitube-loadtest-redis-1", "golitube-loadtest-kafka-1", "golitube-loadtest-mysql-1")) {
  $inspect = Try-Value { (& docker inspect $name) -join "" | ConvertFrom-Json | Select-Object -First 1 }
  if ($inspect) {
    $containers += [ordered]@{
      name = $name
      image = $inspect.Config.Image
      image_id = $inspect.Image
      memory_limit_bytes = $inspect.HostConfig.Memory
      nano_cpus = $inspect.HostConfig.NanoCpus
      cpu_quota = $inspect.HostConfig.CpuQuota
      cpu_period = $inspect.HostConfig.CpuPeriod
      restart_policy = $inspect.HostConfig.RestartPolicy.Name
      network_mode = $inspect.HostConfig.NetworkMode
    }
  }
}

$environment = [ordered]@{
  generated_at = (Get-Date).ToString("o")
  project_root = $ProjectRoot
  git = [ordered]@{
    branch = $gitBranch
    commit = $gitSha
    dirty = [bool]($gitStatus -and $gitStatus.Trim().Length -gt 0)
    status = $gitStatus
  }
  go = $goVersion
  powershell = $PSVersionTable.PSVersion.ToString()
  host = [ordered]@{
    hostname = $env:COMPUTERNAME
    cpu = $cpu.Name
    physical_cores = $cpu.NumberOfCores
    logical_cores = $cpu.NumberOfLogicalProcessors
    max_clock_mhz = $cpu.MaxClockSpeed
    memory_gb = [math]::Round($system.TotalPhysicalMemory / 1GB, 1)
    os = $os.Caption
    os_version = $os.Version
    os_build = $os.BuildNumber
  }
  docker = [ordered]@{
    server_version = Try-Value { (& docker version --format "{{.Server.Version}}") -join "" }
    compose_version = Try-Value { (& docker compose version --format json | ConvertFrom-Json).version }
    vm_cpus = Try-Value { [int](& docker info --format "{{.NCPU}}") }
    vm_memory_bytes = Try-Value { [long](& docker info --format "{{.MemTotal}}") }
    containers = $containers
  }
  deployment = [ordered]@{
    topology = "单机；Go server、persist、压测客户端与 Redis/Kafka/MySQL 容器共用同一台机器"
    server_addr = "127.0.0.1:18081"
    redis_addr = "127.0.0.1:16379"
    kafka_brokers = "127.0.0.1:29092"
    mysql_addr = "127.0.0.1:23306"
    mysql_database = "golitube_loadtest"
    redis_policy = "512MB maxmemory，noeviction，关闭 RDB/AOF"
    kafka_topic = "golitube-danmaku，3 分区，1 副本，保留 1 小时"
    mysql_config = "buffer pool 1GB，max_connections=200"
    notes = "容器资源限制来自 deploy/loadtest/compose.yaml；宿主机仍与容器争用同一组 CPU 和内存"
  }
  binaries = [ordered]@{
    server_sha256 = Try-Value { (Get-FileHash (Join-Path $ProjectRoot "build\golitube.exe") -Algorithm SHA256).Hash }
    loadtest_sha256 = Try-Value { (Get-FileHash (Join-Path $ProjectRoot "build\loadtest.exe") -Algorithm SHA256).Hash }
  }
}

$json = $environment | ConvertTo-Json -Depth 8
$path = Join-Path $OutDir "environment.json"
[System.IO.File]::WriteAllText($path, $json, [System.Text.UTF8Encoding]::new($false))
Write-Host "环境记录写入 $path"
