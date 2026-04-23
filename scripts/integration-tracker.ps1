param(
    [string]$GoExe = "G:\devsoft\Go\bin\go.exe",
    [int]$TrackerPort = 19301,
    [int]$SeedPort = 19311,
    [int]$LeecherPort = 19312,
    [int]$FileSizeMB = 20,
    [switch]$KeepArtifacts
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$runId = [DateTimeOffset]::Now.ToUnixTimeMilliseconds()
$runRoot = Join-Path $projectRoot (".integration-tracker-run-" + $runId)
$binPath = Join-Path $runRoot "p2p.exe"
$sourcePath = Join-Path $runRoot "source.bin"
$out2Path = Join-Path $runRoot "leecher2-out.bin"
$out3Path = Join-Path $runRoot "leecher3-out.bin"
$manifestDir = Join-Path $runRoot ".p2p"
$store2Dir = Join-Path $runRoot ".p2p-store-2"
$store3Dir = Join-Path $runRoot ".p2p-store-3"
$trackerOutLog = Join-Path $runRoot "tracker.out.log"
$trackerErrLog = Join-Path $runRoot "tracker.err.log"
$seedOutLog = Join-Path $runRoot "seed.out.log"
$seedErrLog = Join-Path $runRoot "seed.err.log"
$leecher2OutLog = Join-Path $runRoot "leecher2.out.log"
$leecher2ErrLog = Join-Path $runRoot "leecher2.err.log"
$leecher3OutLog = Join-Path $runRoot "leecher3.out.log"
$leecher3ErrLog = Join-Path $runRoot "leecher3.err.log"
$trackerUrl = "http://127.0.0.1:$TrackerPort"

New-Item -ItemType Directory -Force $runRoot | Out-Null
Set-Location $projectRoot

function Wait-ForListenPort {
    param(
        [int]$Port,
        [int]$TimeoutSeconds = 15
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $listen = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
        if ($listen) {
            return
        }
        Start-Sleep -Milliseconds 100
    } until ((Get-Date) -ge $deadline)

    throw "Timed out waiting for port $Port to start listening"
}

function New-TestFile {
    param(
        [string]$Path,
        [int]$SizeMB
    )

    $size = $SizeMB * 1024 * 1024
    $bytes = New-Object byte[] $size
    for ($i = 0; $i -lt $size; $i++) {
        $bytes[$i] = [byte]($i % 251)
    }
    [System.IO.File]::WriteAllBytes($Path, $bytes)
}

Write-Host "Building binary into $binPath"
& $GoExe build -o $binPath ./cmd/p2p

Write-Host "Generating test file: $sourcePath ($FileSizeMB MiB)"
New-TestFile -Path $sourcePath -SizeMB $FileSizeMB

Write-Host "Generating manifest"
& $binPath share --path $sourcePath --data-dir $manifestDir | Out-Null
$manifestPath = Get-ChildItem -Recurse $manifestDir -Filter manifest.json | Select-Object -First 1 -ExpandProperty FullName
if (-not $manifestPath) {
    throw "manifest.json was not generated"
}

$trackerProcess = $null
$seedProcess = $null
$leecher2Process = $null
$leecher3Process = $null

try {
    Write-Host "Starting tracker on 127.0.0.1:$TrackerPort"
    $trackerProcess = Start-Process -FilePath $binPath `
        -ArgumentList @("tracker", "--listen", "127.0.0.1:$TrackerPort") `
        -WorkingDirectory $projectRoot `
        -RedirectStandardOutput $trackerOutLog `
        -RedirectStandardError $trackerErrLog `
        -PassThru
    Wait-ForListenPort -Port $TrackerPort

    Write-Host "Starting seed on 127.0.0.1:$SeedPort"
    $seedProcess = Start-Process -FilePath $binPath `
        -ArgumentList @("serve", "--path", $sourcePath, "--listen", "127.0.0.1:$SeedPort", "--data-dir", $manifestDir, "--tracker", $trackerUrl) `
        -WorkingDirectory $projectRoot `
        -RedirectStandardOutput $seedOutLog `
        -RedirectStandardError $seedErrLog `
        -PassThru
    Wait-ForListenPort -Port $SeedPort

    Write-Host "Starting leecher2 on 127.0.0.1:$LeecherPort"
    $leecher2Process = Start-Process -FilePath $binPath `
        -ArgumentList @("get", "--manifest", $manifestPath, "--store-dir", $store2Dir, "--out", $out2Path, "--listen", "127.0.0.1:$LeecherPort", "--seed-after-download", "--tracker", $trackerUrl, "--peer", "127.0.0.1:$SeedPort") `
        -WorkingDirectory $projectRoot `
        -RedirectStandardOutput $leecher2OutLog `
        -RedirectStandardError $leecher2ErrLog `
        -PassThru
    Wait-ForListenPort -Port $LeecherPort

    Write-Host "Starting leecher3 with tracker discovery"
    $leecher3Process = Start-Process -FilePath $binPath `
        -ArgumentList @("get", "--manifest", $manifestPath, "--store-dir", $store3Dir, "--out", $out3Path, "--tracker", $trackerUrl) `
        -WorkingDirectory $projectRoot `
        -RedirectStandardOutput $leecher3OutLog `
        -RedirectStandardError $leecher3ErrLog `
        -PassThru

    $leecher3Process | Wait-Process -Timeout 40

    $sourceHash = (Get-FileHash $sourcePath -Algorithm SHA256).Hash
    $out3Hash = (Get-FileHash $out3Path -Algorithm SHA256).Hash

    Write-Host ""
    Write-Host "Verification"
    Write-Host "SOURCE_HASH=$sourceHash"
    Write-Host "OUT3_HASH=$out3Hash"

    if ($sourceHash -ne $out3Hash) {
        throw "hash mismatch between source and leecher3 output"
    }

    Write-Host ""
    Write-Host "Leecher3 log excerpt"
    Select-String -Path $leecher3OutLog -Pattern "tracker_peer_discovered|peer_have_received|piece_downloaded|content_assembled" |
        Select-Object -First 30 |
        ForEach-Object { $_.Line }

    Write-Host ""
    Write-Host "Artifacts: $runRoot"
}
finally {
    foreach ($proc in @($leecher3Process, $leecher2Process, $seedProcess, $trackerProcess)) {
        if ($proc -and -not $proc.HasExited) {
            Stop-Process -Id $proc.Id -Force
        }
    }

    if (-not $KeepArtifacts) {
        Write-Host "Cleaning artifacts"
        Remove-Item -LiteralPath $runRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
}
