# send-events.ps1
# Script untuk mengirim 5000 events dengan 25% duplikasi rate

$topic = "demo.v1"
$now = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

Write-Host "Generating 3750 unique events..." -ForegroundColor Green
$uniq = 0..3749 | ForEach-Object {
    @{
        topic = $topic
        event_id = ("ev-{0:D6}" -f $_)
        timestamp = $now
        source = "ps"
        payload = @{ i = $_ }
    }
}

Write-Host "Generating 1250 duplicate events..." -ForegroundColor Yellow
$dups = 0..1249 | ForEach-Object {
    @{
        topic = $topic
        event_id = ("ev-{0:D6}" -f ($_ % 3750))
        timestamp = $now
        source = "ps"
        payload = @{ msg = "duplicate" }
    }
}

$events = $uniq + $dups
$total = $events.Count
Write-Host "Total events: $total (3750 unique + 1250 duplicates)" -ForegroundColor Cyan

Write-Host "Sending to /publish endpoint..." -ForegroundColor Green
$start = Get-Date
$payload = @{ events = $events }
$response = Invoke-RestMethod -Method POST `
    -Uri "http://localhost:8000/publish" `
    -ContentType "application/json" `
    -Body ($payload | ConvertTo-Json -Depth 7)
$elapsed = (Get-Date) - $start

Write-Host "Response: OK" -ForegroundColor Green
Write-Host ("Time elapsed: " + $elapsed.TotalSeconds + " seconds") -ForegroundColor Green
$throughput = [math]::Round($total / $elapsed.TotalSeconds)
Write-Host ("Throughput: " + $throughput + " events/sec") -ForegroundColor Cyan
