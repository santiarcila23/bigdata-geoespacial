# Benchmark Dask vs Spark (version PowerShell para Windows).
# Agregacion por grilla con dos configuraciones de workers e igual numero de nucleos:
#   Spark: 1 worker x 2 cores  y  2 workers x 2 cores   (2 y 4 nucleos)
#   Dask : 2 workers x 1 hilo  y  4 workers x 1 hilo    (2 y 4 nucleos)
# Requisito: haber corrido la ingesta (crea /data/clean en el volumen compartido).
# Uso (desde la carpeta del proyecto):   powershell -ExecutionPolicy Bypass -File benchmark\run_benchmark.ps1
# Antes de correrlo conviene detener Jenkins para liberar RAM:  docker compose stop jenkins

$ErrorActionPreference = "Continue"
Set-Location (Split-Path $PSScriptRoot -Parent)

$runs = if ($env:RUNS) { $env:RUNS } else { "3" }
$grid = if ($env:GRID) { $env:GRID } else { "0.005" }
$out  = "benchmark\results.jsonl"
New-Item -ItemType File -Path $out -Force | Out-Null

function Save-Bench($lines) {
    foreach ($l in $lines) {
        if ("$l" -match '^BENCH (.*)$') {
            Write-Host "  $($Matches[1])" -ForegroundColor Green
            Add-Content -Path $out -Value $Matches[1] -Encoding ascii
        }
    }
}

Write-Host "== Spark ==" -ForegroundColor Cyan
docker compose stop dask-worker | Out-Null
foreach ($n in 1, 2) {
    Write-Host "-- Spark con $n worker(s)" -ForegroundColor Cyan
    docker compose up -d --scale spark-worker=$n spark-master spark-worker
    $lines = docker compose run --rm -T --no-deps spark-job bash /opt/jobs/submit.sh /opt/jobs/benchmark_grid.py `
        --runs $runs --grid $grid --expect-workers $n --label "spark-$($n)w-$($n * 2)c"
    Save-Bench $lines
}

Write-Host "== Dask ==" -ForegroundColor Cyan
docker compose up -d --scale spark-worker=0 spark-worker
$env:DASK_WORKER_THREADS = "1"
foreach ($n in 2, 4) {
    Write-Host "-- Dask con $n worker(s)" -ForegroundColor Cyan
    docker compose up -d --scale dask-worker=$n dask-scheduler dask-worker
    $lines = docker compose run --rm -T --no-deps ingest python -u benchmark_grid.py `
        --workers $n --runs $runs --grid $grid --label "dask-$($n)w-$($n)c"
    Save-Bench $lines
}
Remove-Item Env:DASK_WORKER_THREADS

Write-Host "== Restaurando configuracion por defecto ==" -ForegroundColor Cyan
docker compose up -d --scale spark-worker=1 --scale dask-worker=2 spark-worker dask-worker

Get-Content $out | docker compose run --rm -T --no-deps ingest python summarize_benchmark.py |
    Out-File -FilePath benchmark\results.md -Encoding utf8
Get-Content benchmark\results.md
Write-Host "Resultados en benchmark\results.jsonl y benchmark\results.md" -ForegroundColor Green
