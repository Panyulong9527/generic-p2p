# generic-p2p

`generic-p2p` is a minimal Go implementation of the `GENERIC_P2P_PROJECT_DESIGN.md` LAN-first P2P file sharing MVP.

Current v0.1 status:

- `share` builds a content-addressed manifest with per-piece hashes.
- `serve` exposes a file over the minimal TCP protocol.
- `get` downloads pieces, stores verified pieces on disk, and can serve completed pieces while downloading.
- `status` reports persisted transfer totals, path stats, and per-peer contribution stats.
- `status --watch` continuously prints refreshed status snapshots for a running download.
- `HAVE` ranges are supported.
- LAN peer discovery is supported over UDP announcements.

## Local Integration Check

Use the PowerShell integration script to run a repeatable three-process flow:

```powershell
pwsh -File .\scripts\integration-local.ps1
```

What it validates:

- one seed serves the full file
- leecher2 downloads from the seed and serves completed pieces
- leecher3 discovers peers over LAN announcements and downloads successfully
- leecher3 output hash matches the original source hash

Optional flags:

```powershell
pwsh -File .\scripts\integration-local.ps1 -KeepArtifacts
pwsh -File .\scripts\integration-local.ps1 -FileSizeMB 32
```

## Tracker Integration Check

Use the tracker integration script to run a repeatable `tracker + seed + leecher + leecher` flow:

```powershell
pwsh -File .\scripts\integration-tracker.ps1
```

What it validates:

- one in-memory tracker is started
- the seed registers itself and joins the swarm
- leecher2 downloads while registering itself with the tracker
- leecher3 discovers peers from the tracker and downloads successfully
- leecher3 output hash matches the original source hash

Optional flags:

```powershell
pwsh -File .\scripts\integration-tracker.ps1 -KeepArtifacts
pwsh -File .\scripts\integration-tracker.ps1 -FileSizeMB 32
```
