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
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, webAppHTML)
}

func (s *Server) handleWebShares(w http.ResponseWriter, r *http.Request) {
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
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/v1/web/shares/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
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
    .status {
      min-height: 24px;
      color: var(--muted);
      font-size: 14px;
    }
    .shares {
      display: grid;
      gap: 10px;
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
    .badge {
      display: inline-block;
      margin-left: 8px;
      color: var(--warm);
      font-size: 12px;
      font-weight: 800;
      text-transform: uppercase;
    }
    @media (max-width: 760px) {
      main { width: min(100vw - 20px, 1120px); padding-top: 18px; }
      .grid { grid-template-columns: 1fr; }
      .share { grid-template-columns: 1fr; }
      .actions { justify-content: flex-start; }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <h1>LAN Share <span class="badge">demo</span></h1>
      <p class="subhead">Open this tracker address from devices on the same LAN. Upload a file here, and other devices can immediately see and download it. This is a test demo; uploaded files are stored on the tracker machine.</p>
    </header>
    <div class="grid">
      <section>
        <h2>Share File</h2>
        <form id="uploadForm">
          <input id="fileInput" name="file" type="file" required>
          <button id="uploadButton" type="submit">Upload and Share</button>
          <div id="uploadStatus" class="status"></div>
        </form>
      </section>
      <section>
        <h2>Current Shares</h2>
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
            '<a class="button secondary" href="' + share.manifestPath + '">manifest</a>' +
          '</div>' +
        '</article>'
      ).join("");
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

    uploadForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!fileInput.files.length) return;
      uploadButton.disabled = true;
      uploadStatus.textContent = "Uploading...";
      try {
        const formData = new FormData();
        formData.append("file", fileInput.files[0]);
        const response = await fetch("/v1/web/shares", { method: "POST", body: formData });
        if (!response.ok) throw new Error(await response.text());
        uploadStatus.textContent = "Shared: " + fileInput.files[0].name;
        fileInput.value = "";
        await refreshShares();
      } catch (error) {
        uploadStatus.textContent = error.message || String(error);
      } finally {
        uploadButton.disabled = false;
      }
    });

    refreshShares().catch((error) => {
      sharesEl.innerHTML = '<div class="empty">' + escapeHTML(error.message || String(error)) + '</div>';
    });
    setInterval(() => refreshShares().catch(() => {}), 3000);
  </script>
</body>
</html>`
