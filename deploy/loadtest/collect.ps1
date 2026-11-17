<#
.SYNOPSIS
  按固定间隔采样 Go 服务、压测客户端、Redis、Kafka、MySQL 的资源与积压指标。

.DESCRIPTION
  输出 CSV 和 JSONL。采样进程本身也与被测服务同机，CPU 百分比按整机逻辑核数归一化。
  每行是一次快照；速率类指标（TPS、积压增长）由分析脚本按相邻两行差分计算。

.PARAMETER ServerPid
  server 模式 golitube.exe 的进程号。persist 进程默认也叫 golitube.exe，必须显式指定，
  否则会把两个进程的 CPU 加在一起。

.PARAMETER StopFile
  该文件出现后采样循环退出。运行器在每个档位结束时不需要它，整轮结束时创建即可。
#>
param(
  [Parameter(Mandatory = $true)][string]$OutDir,
  [Parameter(Mandatory = $true)][int]$ServerPid,
  [int]$IntervalSeconds = 10,
  [string]$StopFile = "",
  [string]$MySqlUser = "golitube",
  [string]$MySqlPassword = "golitube-local",
  [string]$MySqlRootPassword = "golitube-root-local",
  [string]$MySqlDatabase = "golitube_loadtest",
  [int]$MaxMinutes = 480
)

$ErrorActionPreference = "Stop"
New-Item -ItemType Directory -Force $OutDir | Out-Null
$csvPath = Join-Path $OutDir "samples.csv"
$jsonlPath = Join-Path $OutDir "samples.jsonl"
$logicalCpus = [Environment]::ProcessorCount

function Invoke-Docker {
  param([string[]]$Arguments)
  $previous = $ErrorActionPreference
  $ErrorActionPreference = "Continue"
  try {
    $output = & docker @Arguments 2>$null
    $exitCode = $LASTEXITCODE
  } finally {
    $ErrorActionPreference = $previous
  }
  if ($exitCode -ne 0) { throw "docker $($Arguments -join ' ') 失败" }
  return $output
}

function Get-ProcessSample {
  param([int]$ProcessId)
  if ($ProcessId -le 0) { return $null }
  $process = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
  if ($null -eq $process) { return $null }
  return [pscustomobject]@{
    CpuSeconds = $process.TotalProcessorTime.TotalSeconds
    WorkingSetMB = [math]::Round($process.WorkingSet64 / 1MB, 1)
    Threads = $process.Threads.Count
    Handles = $process.HandleCount
  }
}

function Get-RedisInfo {
  $lines = Invoke-Docker @("exec", "golitube-loadtest-redis-1", "redis-cli", "INFO")
  $info = @{}
  foreach ($line in $lines) {
    if ($line -match '^([a-zA-Z0-9_]+):(.+)$') { $info[$Matches[1]] = $Matches[2].Trim() }
  }
  $commands = Invoke-Docker @("exec", "golitube-loadtest-redis-1", "redis-cli", "INFO", "commandstats")
  $publishUsec = $null
  foreach ($line in $commands) {
    if ($line -match '^cmdstat_publish:.*usec_per_call=([0-9.]+)') { $publishUsec = [double]$Matches[1] }
  }
  [pscustomobject]@{
    OpsPerSecond = if ($info.ContainsKey("instantaneous_ops_per_sec")) { [double]$info["instantaneous_ops_per_sec"] } else { $null }
    MemoryMB = if ($info.ContainsKey("used_memory")) { [math]::Round([double]$info["used_memory"] / 1MB, 1) } else { $null }
    ConnectedClients = if ($info.ContainsKey("connected_clients")) { [int]$info["connected_clients"] } else { $null }
    PublishUsecPerCall = $publishUsec
    TotalCommands = if ($info.ContainsKey("total_commands_processed")) { [double]$info["total_commands_processed"] } else { $null }
  }
}

function Get-KafkaLag {
  $output = Invoke-Docker @("exec", "golitube-loadtest-kafka-1", "/opt/kafka/bin/kafka-consumer-groups.sh",
    "--bootstrap-server", "localhost:9092", "--describe", "--group", "golitube-persistence-v1")
  $lag = 0
  foreach ($line in $output) {
    $parts = $line -split '\s+'
    if ($parts.Count -ge 6 -and $parts[5] -match '^\d+$') { $lag += [long]$parts[5] }
  }
  return [long]$lag
}

function Get-MySqlStatus {
  $query = "SHOW GLOBAL STATUS WHERE Variable_name IN ('Com_commit','Innodb_rows_inserted','Innodb_rows_read','Threads_connected','Threads_running','Slow_queries','Innodb_row_lock_time_avg');"
  $lines = Invoke-Docker @("exec", "-e", "MYSQL_PWD=$MySqlRootPassword", "golitube-loadtest-mysql-1",
    "mysql", "-uroot", "-N", "-e", $query)
  $status = @{}
  foreach ($line in $lines) {
    $parts = $line -split "`t"
    if ($parts.Count -ge 2) { $status[$parts[0]] = $parts[1] }
  }
  $transaction = Invoke-Docker @("exec", "-e", "MYSQL_PWD=$MySqlRootPassword", "golitube-loadtest-mysql-1",
    "mysql", "-uroot", "-N", "-e",
    "SELECT SUM_TIMER_WAIT/1000000, COUNT_STAR FROM performance_schema.events_transactions_summary_global_by_event_name WHERE EVENT_NAME='transaction';")
  $transactionParts = ($transaction | Select-Object -First 1) -split "`t"
  [pscustomobject]@{
    Commits = if ($status.ContainsKey("Com_commit")) { [double]$status["Com_commit"] } else { $null }
    RowsInserted = if ($status.ContainsKey("Innodb_rows_inserted")) { [double]$status["Innodb_rows_inserted"] } else { $null }
    ThreadsConnected = if ($status.ContainsKey("Threads_connected")) { [int]$status["Threads_connected"] } else { $null }
    ThreadsRunning = if ($status.ContainsKey("Threads_running")) { [int]$status["Threads_running"] } else { $null }
    TransactionSumUS = if ($transactionParts.Count -ge 2) { [double]$transactionParts[0] } else { $null }
    TransactionCount = if ($transactionParts.Count -ge 2) { [double]$transactionParts[1] } else { $null }
  }
}

function Get-ContainerStats {
  $raw = Invoke-Docker @("stats", "--no-stream", "--format", "{{json .}}", "golitube-loadtest-redis-1", "golitube-loadtest-kafka-1", "golitube-loadtest-mysql-1")
  $result = @{}
  foreach ($line in $raw) {
    $row = $line | ConvertFrom-Json
    $name = $row.Name -replace '^golitube-loadtest-', '' -replace '-1$', ''
    $cpu = [double]($row.CPUPerc -replace '%', '')
    $memParts = ($row.MemUsage -split ' / ')
    $memMB = 0.0
    if ($memParts[0] -match '([0-9.]+)([KMG]i?B)') {
      $value = [double]$Matches[1]
      $unit = $Matches[2]
      switch ($unit) {
        "KiB" { $memMB = $value / 1024 }
        "MiB" { $memMB = $value }
        "GiB" { $memMB = $value * 1024 }
        "B" { $memMB = $value / 1MB }
      }
    }
    $net = $row.NetIO -split ' / '
    $result[$name] = [pscustomobject]@{
      CpuPercent = $cpu
      MemoryMB = [math]::Round($memMB, 1)
      NetIn = $net[0]
      NetOut = $net[1]
    }
  }
  return $result
}

function Get-ServerMetrics {
  $previous = $ErrorActionPreference
  $ErrorActionPreference = "Continue"
  try {
    $metrics = Invoke-RestMethod -Uri "http://127.0.0.1:18081/debug/metrics" -TimeoutSec 3
  } finally {
    $ErrorActionPreference = $previous
  }
  [pscustomobject]@{
    Connections   = $metrics.connections.current
    Goroutines    = $metrics.runtime.goroutines
    HeapMB        = [math]::Round($metrics.runtime.heap_alloc_bytes / 1MB, 1)
    GCCount       = $metrics.runtime.gc_count
    Publishes     = $metrics.messages.publishes
    Deliveries    = $metrics.messages.deliveries
    SlowEvictions = $metrics.messages.slow_conn_evictions
    RateLimited   = $metrics.messages.rate_limited
    Queued        = $metrics.messages.queued_messages
    PublishP99    = $metrics.publish_latency.p99_ms
  }
}

$header = @(
  "time", "elapsed_s",
  "server_cpu_pct", "server_rss_mb", "server_threads",
  "server_conns", "server_goroutines", "server_heap_mb", "server_gc_count",
  "server_publishes", "server_deliveries", "server_slow_evictions", "server_rate_limited",
  "server_queued", "server_publish_p99_ms",
  "load_cpu_pct", "load_rss_mb", "load_threads",
  "redis_cpu_pct", "redis_mem_mb", "redis_ops_per_s", "redis_publish_us",
  "kafka_cpu_pct", "kafka_mem_mb", "kafka_lag",
  "mysql_cpu_pct", "mysql_mem_mb", "mysql_commits", "mysql_rows_inserted",
  "mysql_threads_connected", "mysql_threads_running", "mysql_tx_sum_us", "mysql_tx_count",
  "sample_error"
)
$writer = [System.IO.StreamWriter]::new($csvPath, $false, [System.Text.UTF8Encoding]::new($false))
$writer.WriteLine(($header -join ","))
$writer.Flush()
$jsonl = [System.IO.StreamWriter]::new($jsonlPath, $false, [System.Text.UTF8Encoding]::new($false))

$started = Get-Date
$previous = $null
$previousAt = $null

while ($true) {
  $now = Get-Date
  if ($StopFile -and (Test-Path $StopFile)) { break }
  if (($now - $started).TotalMinutes -gt $MaxMinutes) { break }

  $errorText = ""
  $server = $null
  $client = $null
  $redis = $null
  $kafka = $null
  $mysql = $null
  $serverMetrics = $null
  $stats = @{}

  try { $server = Get-ProcessSample -ProcessId $ServerPid } catch { $errorText += "server:$($_.Exception.Message);" }
  try { $serverMetrics = Get-ServerMetrics } catch { $errorText += "metrics:$($_.Exception.Message);" }
  try {
    $loadProcess = Get-Process -Name "loadtest" -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($loadProcess) { $client = Get-ProcessSample -ProcessId $loadProcess.Id }
  } catch { $errorText += "client:$($_.Exception.Message);" }
  try { $redis = Get-RedisInfo } catch { $errorText += "redis:$($_.Exception.Message);" }
  try { $kafkaLag = Get-KafkaLag } catch { $kafkaLag = $null; $errorText += "kafka:$($_.Exception.Message);" }
  try { $mysql = Get-MySqlStatus } catch { $errorText += "mysql:$($_.Exception.Message);" }
  try { $stats = Get-ContainerStats } catch { $errorText += "stats:$($_.Exception.Message);" }

  $serverCpu = $null
  $loadCpu = $null
  if ($previous -and $previousAt) {
    $seconds = ($now - $previousAt).TotalSeconds
    if ($seconds -gt 0) {
      if ($server -and $previous.Server) {
        $serverCpu = [math]::Round(($server.CpuSeconds - $previous.Server.CpuSeconds) / $seconds / $logicalCpus * 100, 2)
      }
      if ($client -and $previous.Client) {
        $loadCpu = [math]::Round(($client.CpuSeconds - $previous.Client.CpuSeconds) / $seconds / $logicalCpus * 100, 2)
      }
    }
  }

  $row = [ordered]@{
    time = $now.ToString("o")
    elapsed_s = [math]::Round(($now - $started).TotalSeconds, 1)
    server_cpu_pct = $serverCpu
    server_rss_mb = if ($server) { $server.WorkingSetMB } else { $null }
    server_threads = if ($server) { $server.Threads } else { $null }
    server_conns = if ($serverMetrics) { $serverMetrics.Connections } else { $null }
    server_goroutines = if ($serverMetrics) { $serverMetrics.Goroutines } else { $null }
    server_heap_mb = if ($serverMetrics) { $serverMetrics.HeapMB } else { $null }
    server_gc_count = if ($serverMetrics) { $serverMetrics.GCCount } else { $null }
    server_publishes = if ($serverMetrics) { $serverMetrics.Publishes } else { $null }
    server_deliveries = if ($serverMetrics) { $serverMetrics.Deliveries } else { $null }
    server_slow_evictions = if ($serverMetrics) { $serverMetrics.SlowEvictions } else { $null }
    server_rate_limited = if ($serverMetrics) { $serverMetrics.RateLimited } else { $null }
    server_queued = if ($serverMetrics) { $serverMetrics.Queued } else { $null }
    server_publish_p99_ms = if ($serverMetrics) { $serverMetrics.PublishP99 } else { $null }
    load_cpu_pct = $loadCpu
    load_rss_mb = if ($client) { $client.WorkingSetMB } else { $null }
    load_threads = if ($client) { $client.Threads } else { $null }
    redis_cpu_pct = if ($stats.ContainsKey("redis")) { $stats["redis"].CpuPercent } else { $null }
    redis_mem_mb = if ($redis) { $redis.MemoryMB } else { $null }
    redis_ops_per_s = if ($redis) { $redis.OpsPerSecond } else { $null }
    redis_publish_us = if ($redis) { $redis.PublishUsecPerCall } else { $null }
    kafka_cpu_pct = if ($stats.ContainsKey("kafka")) { $stats["kafka"].CpuPercent } else { $null }
    kafka_mem_mb = if ($stats.ContainsKey("kafka")) { $stats["kafka"].MemoryMB } else { $null }
    kafka_lag = $kafkaLag
    mysql_cpu_pct = if ($stats.ContainsKey("mysql")) { $stats["mysql"].CpuPercent } else { $null }
    mysql_mem_mb = if ($stats.ContainsKey("mysql")) { $stats["mysql"].MemoryMB } else { $null }
    mysql_commits = if ($mysql) { $mysql.Commits } else { $null }
    mysql_rows_inserted = if ($mysql) { $mysql.RowsInserted } else { $null }
    mysql_threads_connected = if ($mysql) { $mysql.ThreadsConnected } else { $null }
    mysql_threads_running = if ($mysql) { $mysql.ThreadsRunning } else { $null }
    mysql_tx_sum_us = if ($mysql) { $mysql.TransactionSumUS } else { $null }
    mysql_tx_count = if ($mysql) { $mysql.TransactionCount } else { $null }
    sample_error = $errorText
  }

  $values = foreach ($key in $header) {
    $value = $row[$key]
    if ($null -eq $value) { "" } else { [string]$value }
  }
  $writer.WriteLine(($values -join ","))
  $writer.Flush()
  $jsonl.WriteLine(($row | ConvertTo-Json -Compress))
  $jsonl.Flush()

  $previous = @{ Server = $server; Client = $client }
  $previousAt = $now
  Start-Sleep -Seconds $IntervalSeconds
}

$writer.Close()
$jsonl.Close()
