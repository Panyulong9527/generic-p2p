package tracker

import (
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"generic-p2p/internal/core"
)

const defaultWebDataDir = ".p2p-web"

type WebShare struct {
	ContentID    string `json:"contentId"`
	Name         string `json:"name"`
	Size         int64  `json:"size"`
	Pieces       int    `json:"pieces"`
	PieceSize    int64  `json:"pieceSize"`
	UploadedAt   string `json:"uploadedAt"`
	DownloadPath string `json:"downloadPath"`
	ManifestPath string `json:"manifestPath"`
}

type webShareIndex struct {
	Shares map[string]WebShare `json:"shares"`
}

func (s *Server) WithWebDataDir(dataDir string) *Server {
	s.webDataDir = dataDir
	return s
}

func (s *Server) handleWebApp(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.requireWebAuth(w, r) {
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, webAppHTML)
}

func (s *Server) handleWebShares(w http.ResponseWriter, r *http.Request) {
	if !s.requireWebAuth(w, r) {
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.handleListWebShares(w, r)
	case http.MethodPost:
		s.handleCreateWebShare(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleWebShareFile(w http.ResponseWriter, r *http.Request) {
	if !s.requireWebAuth(w, r) {
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/web/shares/")
	parts := strings.Split(strings.Trim(path, "/"), "/")

	if r.Method == http.MethodDelete {
		if len(parts) != 1 || parts[0] == "" {
			http.NotFound(w, r)
			return
		}
		if err := s.deleteWebShare(parts[0]); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		writeJSON(w, map[string]string{"status": "deleted"})
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if len(parts) != 2 {
		http.NotFound(w, r)
		return
	}

	contentID := parts[0]
	action := parts[1]
	shareDir := filepath.Join(s.effectiveWebDataDir(), "shares", contentID)

	switch action {
	case "download":
		share, err := s.webShare(contentID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", share.Name))
		http.ServeFile(w, r, filepath.Join(shareDir, "source"))
	case "manifest":
		w.Header().Set("Content-Type", "application/json")
		http.ServeFile(w, r, filepath.Join(shareDir, "manifest.json"))
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleListWebShares(w http.ResponseWriter, _ *http.Request) {
	shares, err := s.webShares()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string][]WebShare{"shares": shares})
}

func (s *Server) handleCreateWebShare(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(64 << 20); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "file field is required", http.StatusBadRequest)
		return
	}
	defer file.Close()

	share, err := s.createWebShare(file, header)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, share)
}

func (s *Server) createWebShare(file multipart.File, header *multipart.FileHeader) (WebShare, error) {
	root := s.effectiveWebDataDir()
	incomingDir := filepath.Join(root, "incoming")
	if err := os.MkdirAll(incomingDir, 0o755); err != nil {
		return WebShare{}, err
	}

	tmp, err := os.CreateTemp(incomingDir, "upload-*")
	if err != nil {
		return WebShare{}, err
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()

	if _, err := io.Copy(tmp, file); err != nil {
		_ = tmp.Close()
		return WebShare{}, err
	}
	if err := tmp.Close(); err != nil {
		return WebShare{}, err
	}

	manifest, err := core.BuildManifestFromFile(tmpPath, core.DefaultPieceSize)
	if err != nil {
		return WebShare{}, err
	}
	manifest.Name = safeFileName(header.Filename)
	if len(manifest.Files) == 1 {
		manifest.Files[0].Path = manifest.Name
	}

	shareDir := filepath.Join(root, "shares", manifest.ContentID)
	if err := os.MkdirAll(shareDir, 0o755); err != nil {
		return WebShare{}, err
	}
	sourcePath := filepath.Join(shareDir, "source")
	if err := replaceFile(tmpPath, sourcePath); err != nil {
		return WebShare{}, err
	}
	if err := core.WriteManifestFile(filepath.Join(shareDir, "manifest.json"), manifest); err != nil {
		return WebShare{}, err
	}

	share := WebShare{
		ContentID:    manifest.ContentID,
		Name:         manifest.Name,
		Size:         manifest.TotalSize,
		Pieces:       len(manifest.Pieces),
		PieceSize:    manifest.PieceSize,
		UploadedAt:   time.Now().Format(time.RFC3339),
		DownloadPath: "/v1/web/shares/" + manifest.ContentID + "/download",
		ManifestPath: "/v1/web/shares/" + manifest.ContentID + "/manifest",
	}
	return share, s.saveWebShare(share)
}

func (s *Server) webShares() ([]WebShare, error) {
	index, err := s.loadWebShareIndex()
	if err != nil {
		return nil, err
	}

	shares := make([]WebShare, 0, len(index.Shares))
	for _, share := range index.Shares {
		shares = append(shares, share)
	}
	sort.Slice(shares, func(i, j int) bool {
		return shares[i].UploadedAt > shares[j].UploadedAt
	})
	return shares, nil
}

func (s *Server) webShare(contentID string) (WebShare, error) {
	index, err := s.loadWebShareIndex()
	if err != nil {
		return WebShare{}, err
	}
	share, ok := index.Shares[contentID]
	if !ok {
		return WebShare{}, fmt.Errorf("share not found: %s", contentID)
	}
	return share, nil
}

func (s *Server) saveWebShare(share WebShare) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	index, err := s.loadWebShareIndexLocked()
	if err != nil {
		return err
	}
	index.Shares[share.ContentID] = share
	return s.saveWebShareIndexLocked(index)
}

func (s *Server) deleteWebShare(contentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	index, err := s.loadWebShareIndexLocked()
	if err != nil {
		return err
	}
	if _, ok := index.Shares[contentID]; !ok {
		return fmt.Errorf("share not found: %s", contentID)
	}
	delete(index.Shares, contentID)

	shareDir := filepath.Join(s.effectiveWebDataDir(), "shares", contentID)
	if err := os.RemoveAll(shareDir); err != nil {
		return err
	}
	return s.saveWebShareIndexLocked(index)
}

func (s *Server) loadWebShareIndex() (webShareIndex, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadWebShareIndexLocked()
}

func (s *Server) loadWebShareIndexLocked() (webShareIndex, error) {
	index := webShareIndex{Shares: make(map[string]WebShare)}
	data, err := os.ReadFile(s.webShareIndexPath())
	if err != nil {
		if os.IsNotExist(err) {
			return index, nil
		}
		return index, err
	}
	if err := json.Unmarshal(data, &index); err != nil {
		return index, err
	}
	if index.Shares == nil {
		index.Shares = make(map[string]WebShare)
	}
	return index, nil
}

func (s *Server) saveWebShareIndexLocked(index webShareIndex) error {
	if err := os.MkdirAll(s.effectiveWebDataDir(), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.webShareIndexPath(), data, 0o644)
}

func (s *Server) webShareIndexPath() string {
	return filepath.Join(s.effectiveWebDataDir(), "shares.json")
}

func (s *Server) effectiveWebDataDir() string {
	if strings.TrimSpace(s.webDataDir) == "" {
		return defaultWebDataDir
	}
	return s.webDataDir
}

func safeFileName(name string) string {
	name = filepath.Base(strings.TrimSpace(name))
	if name == "." || name == string(filepath.Separator) || name == "" {
		return "shared-file"
	}
	return name
}

func replaceFile(source string, target string) error {
	if err := os.Rename(source, target); err == nil {
		return nil
	}

	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(target)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

const webAppHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>generic-p2p LAN Share</title>
  <style>
    :root {
      --ink: #16201b;
      --muted: #5d6b63;
      --line: #cad6ce;
      --paper: #f4f0e8;
      --panel: #fffaf1;
      --accent: #176d4d;
      --accent-strong: #0d4f37;
      --warm: #c75f2a;
      --shadow: 0 18px 50px rgba(31, 45, 38, 0.12);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
      background:
        linear-gradient(135deg, rgba(23, 109, 77, 0.12), transparent 42%),
        repeating-linear-gradient(90deg, rgba(22, 32, 27, 0.035) 0 1px, transparent 1px 48px),
        var(--paper);
    }
    main {
      width: min(1120px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 32px 0 48px;
    }
    header {
      display: grid;
      gap: 10px;
      padding: 20px 0 24px;
      border-bottom: 1px solid var(--line);
    }
    h1 {
      margin: 0;
      font-size: clamp(30px, 5vw, 56px);
      line-height: 1;
      letter-spacing: 0;
    }
    .subhead {
      margin: 0;
      max-width: 780px;
      color: var(--muted);
      font-size: 16px;
      line-height: 1.6;
    }
    .grid {
      display: grid;
      grid-template-columns: 360px 1fr;
      gap: 18px;
      margin-top: 24px;
      align-items: start;
    }
    .stack {
      display: grid;
      gap: 18px;
    }
    section {
      background: rgba(255, 250, 241, 0.9);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
      padding: 18px;
    }
    h2 {
      margin: 0 0 14px;
      font-size: 18px;
      letter-spacing: 0;
    }
    form {
      display: grid;
      gap: 12px;
    }
    input[type="file"] {
      width: 100%;
      padding: 14px;
      border: 1px dashed var(--accent);
      border-radius: 8px;
      background: #fffdf8;
      color: var(--ink);
    }
    .dropzone {
      display: grid;
      gap: 10px;
      padding: 16px;
      border: 1px dashed var(--accent);
      border-radius: 8px;
      background: #fffdf8;
    }
    .dropzone.dragging {
      border-color: var(--warm);
      background: #fff4e8;
    }
    .drop-hint {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
    }
    progress {
      width: 100%;
      height: 14px;
      accent-color: var(--accent);
    }
    button, .button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 42px;
      padding: 0 14px;
      border: 0;
      border-radius: 6px;
      background: var(--accent);
      color: #fff;
      font-weight: 700;
      text-decoration: none;
      cursor: pointer;
    }
    button:hover, .button:hover { background: var(--accent-strong); }
    button.secondary, .button.secondary {
      background: #e8eee9;
      color: var(--ink);
    }
    button.danger {
      background: #9f3a2f;
      color: #fff;
    }
    button.danger:hover {
      background: #7a2b23;
    }
    .status {
      min-height: 24px;
      color: var(--muted);
      font-size: 14px;
    }
    .shares {
      display: grid;
      gap: 10px;
    }
    .section-title {
      display: flex;
      gap: 10px;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 14px;
    }
    .section-title h2 {
      margin: 0;
    }
    .header-actions {
      display: flex;
      gap: 10px;
      align-items: center;
      justify-content: space-between;
    }
    .share {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 12px;
      align-items: center;
      padding: 14px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fffdf8;
    }
    .name {
      overflow-wrap: anywhere;
      font-weight: 800;
    }
    .meta {
      margin-top: 6px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.5;
    }
    .actions {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      justify-content: flex-end;
    }
    .empty {
      padding: 22px;
      border: 1px dashed var(--line);
      border-radius: 8px;
      color: var(--muted);
      background: rgba(255, 255, 255, 0.45);
    }
    .tracker {
      display: grid;
      gap: 10px;
    }
    .metric-row {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 8px;
    }
    .metric {
      padding: 10px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fffdf8;
    }
    .metric strong {
      display: block;
      font-size: 20px;
    }
    .metric span {
      color: var(--muted);
      font-size: 12px;
    }
    .swarm {
      padding: 10px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fffdf8;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
      overflow-wrap: anywhere;
    }
    .subsection {
      display: grid;
      gap: 8px;
      margin-top: 10px;
    }
    .subsection h3 {
      margin: 0;
      font-size: 13px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
    }
    .badge {
      display: inline-block;
      margin-left: 8px;
      color: var(--warm);
      font-size: 12px;
      font-weight: 800;
      text-transform: uppercase;
    }
    .meta-inline {
      color: var(--muted);
      font-size: 12px;
    }
    .chip {
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      padding: 0 8px;
      border-radius: 999px;
      font-size: 11px;
      font-weight: 800;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      border: 1px solid transparent;
    }
    .chip-ok {
      color: #0d4f37;
      background: rgba(23, 109, 77, 0.12);
      border-color: rgba(23, 109, 77, 0.25);
    }
    .chip-timeout {
      color: #8a4a18;
      background: rgba(199, 95, 42, 0.12);
      border-color: rgba(199, 95, 42, 0.25);
    }
    .chip-fail {
      color: #7a2b23;
      background: rgba(159, 58, 47, 0.12);
      border-color: rgba(159, 58, 47, 0.25);
    }
    .chip-idle {
      color: var(--muted);
      background: rgba(93, 107, 99, 0.08);
      border-color: rgba(93, 107, 99, 0.18);
    }
    .chip-route-udp {
      color: #0d4f37;
      background: rgba(23, 109, 77, 0.12);
      border-color: rgba(23, 109, 77, 0.25);
    }
    .chip-route-mixed {
      color: #8a4a18;
      background: rgba(199, 95, 42, 0.12);
      border-color: rgba(199, 95, 42, 0.25);
    }
    .chip-route-tcp {
      color: #7a2b23;
      background: rgba(159, 58, 47, 0.12);
      border-color: rgba(159, 58, 47, 0.25);
    }
    .chip-route-none {
      color: var(--muted);
      background: rgba(93, 107, 99, 0.08);
      border-color: rgba(93, 107, 99, 0.18);
    }
    @media (max-width: 760px) {
      main { width: min(100vw - 20px, 1120px); padding-top: 18px; }
      .grid { grid-template-columns: 1fr; }
      .share { grid-template-columns: 1fr; }
      .actions { justify-content: flex-start; }
      .metric-row { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <div class="header-actions">
        <h1>LAN Share <span class="badge">demo</span></h1>
        <form method="post" action="/logout">
          <button class="secondary" type="submit">Logout</button>
        </form>
      </div>
      <p class="subhead">Open this tracker address from devices on the same LAN. Upload a file here, and other devices can immediately see and download it. This is a test demo; uploaded files are stored on the tracker machine.</p>
    </header>
    <div class="grid">
      <div class="stack">
        <section>
          <h2>Share File</h2>
          <form id="uploadForm">
            <label id="dropzone" class="dropzone">
              <input id="fileInput" name="file" type="file" required>
              <span class="drop-hint">Drop a file here or choose one from this device.</span>
            </label>
            <progress id="uploadProgress" value="0" max="100" hidden></progress>
            <button id="uploadButton" type="submit">Upload and Share</button>
            <div id="uploadStatus" class="status"></div>
          </form>
        </section>
        <section>
          <div class="section-title">
            <h2>Tracker</h2>
            <button id="refreshButton" class="secondary" type="button">Refresh</button>
          </div>
          <div id="trackerStatus" class="tracker">
            <div class="empty">Loading tracker status...</div>
          </div>
        </section>
      </div>
      <section>
        <div class="section-title">
          <h2>Current Shares</h2>
        </div>
        <div id="shares" class="shares">
          <div class="empty">Loading shares...</div>
        </div>
      </section>
    </div>
  </main>
  <script>
    const sharesEl = document.querySelector("#shares");
    const uploadForm = document.querySelector("#uploadForm");
    const fileInput = document.querySelector("#fileInput");
    const uploadButton = document.querySelector("#uploadButton");
    const uploadStatus = document.querySelector("#uploadStatus");
    const uploadProgress = document.querySelector("#uploadProgress");
    const dropzone = document.querySelector("#dropzone");
    const refreshButton = document.querySelector("#refreshButton");
    const trackerStatusEl = document.querySelector("#trackerStatus");

    function formatBytes(value) {
      if (value < 1024) return value + " B";
      const units = ["KiB", "MiB", "GiB", "TiB"];
      let size = value / 1024;
      let unit = 0;
      while (size >= 1024 && unit < units.length - 1) {
        size /= 1024;
        unit += 1;
      }
      return size.toFixed(1) + " " + units[unit];
    }

    function renderShares(shares) {
      if (!shares.length) {
        sharesEl.innerHTML = '<div class="empty">No shared files yet.</div>';
        return;
      }
      sharesEl.innerHTML = shares.map((share) =>
        '<article class="share">' +
          '<div>' +
            '<div class="name">' + escapeHTML(share.name) + '</div>' +
            '<div class="meta">' +
              formatBytes(share.size) + ' / ' + share.pieces + ' pieces / ' + new Date(share.uploadedAt).toLocaleString() +
              '<br>' + escapeHTML(share.contentId) +
            '</div>' +
          '</div>' +
          '<div class="actions">' +
            '<a class="button" href="' + share.downloadPath + '">Download</a>' +
            '<button class="secondary" type="button" data-action="copy" data-url="' + escapeHTML(share.downloadPath) + '">Copy link</button>' +
            '<a class="button secondary" href="' + share.manifestPath + '">manifest</a>' +
            '<button class="danger" type="button" data-action="delete" data-content-id="' + escapeHTML(share.contentId) + '">Delete</button>' +
          '</div>' +
        '</article>'
      ).join("");
    }

    function renderTrackerStatus(status) {
      const swarms = status.swarms || [];
      const pendingUdpProbes = status.pendingUdpProbes || [];
      const udpProbeResults = status.udpProbeResults || [];
      const peerTransferPaths = status.peerTransferPaths || [];
      const udpProbeResultMap = Object.fromEntries(
        udpProbeResults.map((item) => [item.targetPeerId || "", item])
      );
      const peerTransferPathMap = Object.fromEntries(
        peerTransferPaths.map((item) => [item.targetPeerId || "", item])
      );
      const swarmHTML = swarms.length
        ? swarms.map((swarm) =>
          '<div class="swarm">' +
            '<strong>' + escapeHTML(shortContentId(swarm.contentId)) + '</strong><br>' +
            swarm.peerCount + ' peers' +
            '<div class="subsection">' +
              renderSwarmPeers(swarm.peers || [], udpProbeResultMap, peerTransferPathMap) +
            '</div>' +
          '</div>'
        ).join("")
        : '<div class="empty">No active P2P swarms.</div>';
      const pendingHTML = pendingUdpProbes.length
        ? '<div class="subsection">' +
            '<h3>Pending UDP Probe Tasks</h3>' +
            pendingUdpProbes.map((item) =>
              '<div class="swarm">' +
                '<strong>' + escapeHTML(item.targetPeerId || "peer") + '</strong><br>' +
                item.requestCount + ' queued coordination requests' +
              '</div>'
            ).join("") +
          '</div>'
        : '';
      const udpResultHTML = udpProbeResults.length
        ? '<div class="subsection">' +
            '<h3>UDP Probe Results</h3>' +
            udpProbeResults.map((item) =>
              '<div class="swarm">' +
                '<strong>' + escapeHTML(item.targetPeerId || "peer") + '</strong> ' + formatProbeResultChip(item) + '<br>' +
                'success ' + (item.successCount || 0) + ' / failure ' + (item.failureCount || 0) +
                '<br>last success ' + formatTimestamp(item.lastSuccessAt) +
                '<br>last failure ' + formatTimestamp(item.lastFailureAt) +
                '<br>last error ' + escapeHTML(item.lastErrorKind || "-") +
                '<br>requester ' + escapeHTML(item.lastRequesterPeerId || "-") +
                '<br>content ' + escapeHTML(shortContentId(item.lastContentId || "-")) +
              '</div>'
            ).join("") +
          '</div>'
        : '';

      trackerStatusEl.innerHTML =
        '<div class="metric-row">' +
          '<div class="metric"><strong>' + status.peerCount + '</strong><span>peers</span></div>' +
          '<div class="metric"><strong>' + status.swarmCount + '</strong><span>swarms</span></div>' +
          '<div class="metric"><strong>' + (status.pendingUdpProbeCount || 0) + '</strong><span>pending UDP probes</span></div>' +
        '</div>' +
        '<div class="metric-row">' +
          '<div class="metric"><strong>' + (status.recentUdpProbeSuccesses || 0) + '</strong><span>UDP probe success</span></div>' +
          '<div class="metric"><strong>' + (status.recentUdpProbeFailures || 0) + '</strong><span>UDP probe failure</span></div>' +
          '<div class="metric"><strong>' + status.peerTtlSeconds + 's</strong><span>peer TTL</span></div>' +
        '</div>' +
        '<div class="metric-row">' +
          '<div class="metric"><strong>' + status.cleanupIntervalSeconds + 's</strong><span>cleanup interval</span></div>' +
          '<div class="metric"><strong>' + escapeHTML(status.statePath || "-") + '</strong><span>state file</span></div>' +
          '<div class="metric"><strong>' + peerTransferPaths.length + '</strong><span>tracked transfer peers</span></div>' +
        '</div>' +
        pendingHTML +
        udpResultHTML +
        swarmHTML;
    }

    function shortContentId(contentId) {
      if (!contentId || contentId.length <= 20) return contentId || "";
      return contentId.slice(0, 20) + "...";
    }

    function peerSummary(peers) {
      if (!peers.length) return "no peers";
      return peers.map((peer) => escapeHTML(peer.peerId || "peer")).join(", ");
    }

    function renderSwarmPeers(peers, udpProbeResultMap, peerTransferPathMap) {
      if (!peers.length) return '<div class="empty">No peers</div>';
      return peers.map((peer) => {
        const result = udpProbeResultMap[peer.peerId || ""] || null;
        const transfer = peerTransferPathMap[peer.peerId || ""] || null;
        return '<div class="swarm">' +
          '<strong>' + escapeHTML(peer.peerId || "peer") + '</strong> ' + formatProbeResultChip(result) + ' ' + formatRouteChip(peer, result) + '<br>' +
          'actual ' + formatActualPathChip(transfer) + pathTotalsSuffix(transfer) +
          'udp ' + escapeHTML((peer.udpAddrs || []).join(",") || "-") +
          '<br>observed ' + escapeHTML(peer.observedUdpAddr || "-") +
          '<br>have ' + escapeHTML(formatHaveRanges(peer.haveRanges || [])) +
        '</div>';
      }).join("");
    }

    function formatTimestamp(value) {
      if (!value) return "-";
      return new Date(value * 1000).toLocaleString();
    }

    function formatProbeResultChip(item) {
      if (!item) {
        return '<span class="chip chip-idle">no result</span>';
      }
      const lastSuccess = item.lastSuccessAt || 0;
      const lastFailure = item.lastFailureAt || 0;
      if (lastSuccess > lastFailure) {
        return '<span class="chip chip-ok">recent success</span>';
      }
      if (lastFailure > 0) {
        const kind = String(item.lastErrorKind || "").trim();
        if (kind === "udp_timeout") {
          return '<span class="chip chip-timeout">udp timeout</span>';
        }
        return '<span class="chip chip-fail">' + escapeHTML(kind || "failure") + '</span>';
      }
      return '<span class="chip chip-idle">no result</span>';
    }

    function formatRouteChip(peer, result) {
      const advice = peerRouteAdvice(peer, result);
      return '<span class="chip ' + advice.className + '">' + escapeHTML(advice.label) + '</span>';
    }

    function formatActualPathChip(item) {
      if (!item || !item.lastPath) {
        return '<span class="chip chip-idle">unknown</span>';
      }
      if (item.lastPath === "udp") {
        return '<span class="chip chip-route-udp">recent udp</span>';
      }
      if (item.lastPath === "tcp") {
        return '<span class="chip chip-route-tcp">recent tcp</span>';
      }
      return '<span class="chip chip-idle">' + escapeHTML(item.lastPath) + '</span>';
    }

    function pathTotalsSuffix(item) {
      if (!item) {
        return '<br>';
      }
      return ' <span class="meta-inline">udp ' + Number(item.udpCount || 0) + ' / tcp ' + Number(item.tcpCount || 0) + ' / last ' + escapeHTML(formatTimestamp(item.lastAt)) + '</span><br>';
    }

    function peerRouteAdvice(peer, result) {
      const udpAddrs = peer && Array.isArray(peer.udpAddrs) ? peer.udpAddrs.filter(Boolean) : [];
      const hasObservedUDP = Boolean(peer && peer.observedUdpAddr);
      const hasUDPPath = udpAddrs.length > 0 || hasObservedUDP;
      if (!hasUDPPath) {
        return { className: "chip-route-none", label: "tcp only" };
      }
      if (!result) {
        return hasObservedUDP
          ? { className: "chip-route-udp", label: "prefer udp" }
          : { className: "chip-route-mixed", label: "try udp" };
      }

      const lastSuccess = Number(result.lastSuccessAt || 0);
      const lastFailure = Number(result.lastFailureAt || 0);
      if (lastSuccess > lastFailure) {
        return { className: "chip-route-udp", label: "prefer udp" };
      }
      if (lastFailure > 0) {
        const kind = String(result.lastErrorKind || "").trim();
        if (kind === "udp_timeout") {
          return { className: "chip-route-mixed", label: "udp fallback" };
        }
        return { className: "chip-route-tcp", label: "prefer tcp" };
      }
      return hasObservedUDP
        ? { className: "chip-route-udp", label: "prefer udp" }
        : { className: "chip-route-mixed", label: "try udp" };
    }

    function formatHaveRanges(ranges) {
      if (!ranges.length) return "none";
      return ranges.map((item) => {
        if (item.start === item.end) return String(item.start);
        return String(item.start) + "-" + String(item.end);
      }).join(",");
    }

    function escapeHTML(value) {
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
    }

    async function refreshShares() {
      const response = await fetch("/v1/web/shares");
      if (!response.ok) throw new Error(await response.text());
      const body = await response.json();
      renderShares(body.shares || []);
    }

    async function refreshTrackerStatus() {
      const response = await fetch("/v1/status");
      if (!response.ok) throw new Error(await response.text());
      renderTrackerStatus(await response.json());
    }

    async function deleteShare(contentId) {
      const response = await fetch("/v1/web/shares/" + encodeURIComponent(contentId), { method: "DELETE" });
      if (!response.ok) throw new Error(await response.text());
    }

    async function copyText(value) {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(value);
        return;
      }
      const input = document.createElement("input");
      input.value = value;
      document.body.appendChild(input);
      input.select();
      document.execCommand("copy");
      input.remove();
    }

    function uploadSelectedFile(file) {
      return new Promise((resolve, reject) => {
        const request = new XMLHttpRequest();
        const formData = new FormData();
        formData.append("file", file);

        request.open("POST", "/v1/web/shares");
        request.upload.addEventListener("progress", (event) => {
          if (!event.lengthComputable) return;
          uploadProgress.hidden = false;
          uploadProgress.value = Math.round((event.loaded / event.total) * 100);
        });
        request.addEventListener("load", () => {
          if (request.status < 200 || request.status >= 300) {
            reject(new Error(request.responseText || request.statusText));
            return;
          }
          resolve(JSON.parse(request.responseText));
        });
        request.addEventListener("error", () => reject(new Error("upload failed")));
        request.send(formData);
      });
    }

    uploadForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!fileInput.files.length) return;
      uploadButton.disabled = true;
      uploadStatus.textContent = "Uploading...";
      uploadProgress.hidden = false;
      uploadProgress.value = 0;
      try {
        await uploadSelectedFile(fileInput.files[0]);
        uploadStatus.textContent = "Shared: " + fileInput.files[0].name;
        fileInput.value = "";
        await refreshShares();
        await refreshTrackerStatus();
      } catch (error) {
        uploadStatus.textContent = error.message || String(error);
      } finally {
        uploadButton.disabled = false;
        uploadProgress.hidden = true;
      }
    });

    for (const eventName of ["dragenter", "dragover"]) {
      dropzone.addEventListener(eventName, (event) => {
        event.preventDefault();
        dropzone.classList.add("dragging");
      });
    }
    for (const eventName of ["dragleave", "drop"]) {
      dropzone.addEventListener(eventName, (event) => {
        event.preventDefault();
        dropzone.classList.remove("dragging");
      });
    }
    dropzone.addEventListener("drop", (event) => {
      if (!event.dataTransfer.files.length) return;
      fileInput.files = event.dataTransfer.files;
      uploadStatus.textContent = "Ready: " + event.dataTransfer.files[0].name;
    });
    sharesEl.addEventListener("click", async (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;

      const action = target.dataset.action;
      if (action === "copy") {
        const fullURL = new URL(target.dataset.url || "", window.location.href).toString();
        try {
          await copyText(fullURL);
          uploadStatus.textContent = "Copied: " + fullURL;
        } catch (error) {
          uploadStatus.textContent = error.message || String(error);
        }
        return;
      }

      if (action === "delete") {
        const contentId = target.dataset.contentId || "";
        if (!contentId || !window.confirm("Delete this shared file from the tracker machine?")) return;
        target.disabled = true;
        try {
          await deleteShare(contentId);
          uploadStatus.textContent = "Deleted share.";
          await refreshShares();
        } catch (error) {
          uploadStatus.textContent = error.message || String(error);
        } finally {
          target.disabled = false;
        }
      }
    });
    refreshButton.addEventListener("click", () => refreshAll().catch((error) => {
      trackerStatusEl.innerHTML = '<div class="empty">' + escapeHTML(error.message || String(error)) + '</div>';
    }));

    async function refreshAll() {
      await Promise.all([refreshShares(), refreshTrackerStatus()]);
    }

    refreshShares().catch((error) => {
      sharesEl.innerHTML = '<div class="empty">' + escapeHTML(error.message || String(error)) + '</div>';
    });
    refreshTrackerStatus().catch((error) => {
      trackerStatusEl.innerHTML = '<div class="empty">' + escapeHTML(error.message || String(error)) + '</div>';
    });
    setInterval(() => refreshAll().catch(() => {}), 3000);
  </script>
</body>
</html>`
