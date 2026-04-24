# generic-p2p

`generic-p2p` is a minimal Go implementation of a generic P2P file sharing engine based on `GENERIC_P2P_PROJECT_DESIGN.md`.

The current codebase is no longer just a LAN-only smoke prototype. It contains a working LAN MVP, a thin tracker path, concurrent multi-peer downloading, basic scheduling resilience, and persisted runtime observability.

## Current Capabilities

- `share` builds a content-addressed manifest with per-piece SHA-256 hashes.
- `serve` exposes a complete local file over the minimal TCP peer protocol.
- `get` downloads verified pieces into a disk-backed piece store and assembles the final output.
- `get --listen` lets a downloader serve completed pieces while downloading.
- `get --seed-after-download` keeps a completed downloader alive as a peer.
- `HAVE` ranges let peers advertise completed piece ranges.
- LAN discovery works through UDP announcements.
- `tracker` starts a thin tracker for peer discovery, can persist swarm state to a local JSON file, and serves a demo LAN sharing webpage.
- `tracker-status` inspects live tracker state, including active swarms and peer ranges.
- `serve --tracker` and `get --tracker` register and join swarms through the tracker.
- Downloads support concurrent workers, dynamic peer refresh, rarest-first piece selection, peer cooldown, peer load tracking, and peer usage balancing.
- `status` reports persisted progress, transfer totals, path stats, per-peer contribution stats, and active downloads.
- `status --pretty --watch` provides a human-readable live view of a running download.

## Commands

Share a file and write a manifest:

```powershell
p2p share --path .\file.bin --data-dir .\.p2p
```

Serve a complete source file:

```powershell
p2p serve --path .\file.bin --listen 127.0.0.1:9001 --data-dir .\.p2p
```

Download from an explicit peer:

```powershell
p2p get --manifest .\.p2p\<contentId>\manifest.json --store-dir .\.p2p-store --out .\out.bin --peer 127.0.0.1:9001
```

Download while also serving completed pieces:

```powershell
p2p get --manifest .\.p2p\<contentId>\manifest.json --store-dir .\.p2p-store --out .\out.bin --listen 127.0.0.1:9002 --peer 127.0.0.1:9001
```

Keep a completed downloader alive as a seed:

```powershell
p2p get --manifest .\.p2p\<contentId>\manifest.json --store-dir .\.p2p-store --out .\out.bin --listen 127.0.0.1:9002 --seed-after-download --peer 127.0.0.1:9001
```

Start the thin tracker:

```powershell
p2p tracker --listen 127.0.0.1:7000
p2p tracker --listen 127.0.0.1:7000 --state-file .\.p2p-tracker-state.json
p2p tracker --listen 0.0.0.0:7000 --web-data-dir .\.p2p-web
p2p tracker --listen 127.0.0.1:7000 --peer-ttl 30s --cleanup-interval 5s
```

Open the demo LAN sharing page:

```text
http://127.0.0.1:7000/
```

For another device on the same LAN, replace `127.0.0.1` with the tracker machine's LAN IP address.

Inspect tracker state:

```powershell
p2p tracker-status --tracker http://127.0.0.1:7000 --pretty
p2p tracker-status --tracker http://127.0.0.1:7000 --watch --interval 1s
```

Use tracker discovery:

```powershell
p2p serve --path .\file.bin --listen 127.0.0.1:9001 --data-dir .\.p2p --tracker http://127.0.0.1:7000
p2p get --manifest .\.p2p\<contentId>\manifest.json --store-dir .\.p2p-store --out .\out.bin --tracker http://127.0.0.1:7000
```

Watch status:

```powershell
p2p status --manifest .\.p2p\<contentId>\manifest.json --store-dir .\.p2p-store --pretty
p2p status --manifest .\.p2p\<contentId>\manifest.json --store-dir .\.p2p-store --watch --interval 1s
p2p status --manifest .\.p2p\<contentId>\manifest.json --store-dir .\.p2p-store --watch --interval 1s --no-clear
```

Example pretty status:

```text
content=sha256-99254018a4506... state=completed progress=100.0% pieces=20/20 peers=2
traffic down=20.0 MiB up=0 B downRate=1.0 MiB/s upRate=0 B/s
path lan=20.0 MiB direct=0 B relay=0 B
peerStats
  127.0.0.1:19311 down=10.0 MiB (10 pieces) up=0 B (0 pieces)
  127.0.0.1:19312 down=10.0 MiB (10 pieces) up=0 B (0 pieces)
```

When a download is running, the pretty view can also include active worker state:

```text
activeDownloads
  worker=0 piece=12 peer=127.0.0.1:19311 age=3s started=2026-04-23T15:30:00+08:00
```

## Local Integration Check

Use the PowerShell integration script to run a repeatable LAN flow:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\integration-local.ps1
```

What it validates:

- one seed serves the full file
- leecher2 downloads from the seed and serves completed pieces
- leecher3 discovers peers over LAN announcements
- leecher3 downloads successfully
- leecher3 output hash matches the original source hash

Optional flags:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\integration-local.ps1 -KeepArtifacts
powershell -ExecutionPolicy Bypass -File .\scripts\integration-local.ps1 -FileSizeMB 32
```

## Tracker Integration Check

Use the tracker integration script to run a repeatable `tracker + seed + leecher2 + leecher3` flow:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\integration-tracker.ps1
```

What it validates:

- one in-memory tracker is started
- the seed registers itself and joins the swarm
- leecher2 downloads from the seed and keeps serving after completion
- leecher3 discovers both seed and leecher2 from the tracker
- leecher3 performs real multi-peer downloading
- leecher3 output hash matches the original source hash

Optional flags:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\integration-tracker.ps1 -KeepArtifacts
powershell -ExecutionPolicy Bypass -File .\scripts\integration-tracker.ps1 -FileSizeMB 32
```

## Code Layout

- `cmd/p2p/main.go`: CLI dispatch and usage.
- `cmd/p2p/commands.go`: command handlers for `share`, `get`, `serve`, and `tracker`.
- `cmd/p2p/tracker_status.go`: tracker state rendering and watch mode.
- `cmd/p2p/download.go`: concurrent download execution.
- `cmd/p2p/discovery.go`: explicit peer, LAN, tracker, and dynamic peer candidate discovery.
- `cmd/p2p/status.go`: JSON and pretty status rendering.
- `cmd/p2p/tracker_sync.go`: tracker registration and swarm sync.
- `internal/core`: manifests, pieces, ranges, stores, and runtime stats.
- `internal/net`: TCP peer protocol, LAN discovery, client, and server.
- `internal/scheduler`: piece and peer selection.
- `internal/tracker`: in-memory thin tracker.
- `scripts`: repeatable local integration checks.

## Verification

Run the full test suite:

```powershell
go test ./...
```
