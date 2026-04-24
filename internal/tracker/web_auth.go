package tracker

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

const webSessionCookieName = "p2p_web_session"

type webUserFile struct {
	Users []webUser `json:"users"`
}

type webUser struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func (s *Server) WithWebUsersPath(path string) *Server {
	s.webUsersPath = path
	return s
}

func (s *Server) loadWebUsers() error {
	if strings.TrimSpace(s.webUsersPath) == "" {
		return nil
	}

	data, err := os.ReadFile(s.webUsersPath)
	if err != nil {
		return err
	}

	var file webUserFile
	if err := json.Unmarshal(data, &file); err != nil {
		return err
	}
	users := make(map[string]string, len(file.Users))
	for _, user := range file.Users {
		username := strings.TrimSpace(user.Username)
		if username == "" || user.Password == "" {
			return fmt.Errorf("web user file contains an empty username or password")
		}
		users[username] = user.Password
	}
	if len(users) == 0 {
		return fmt.Errorf("web user file must contain at least one user")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.webUsers = users
	return nil
}

func (s *Server) webAuthEnabled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.webUsers) > 0
}

func (s *Server) authenticateWebUser(username string, password string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	expected, ok := s.webUsers[username]
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(expected), []byte(password)) == 1
}

func (s *Server) createWebSession(username string) (string, error) {
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", err
	}
	token := hex.EncodeToString(tokenBytes)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.webSessions == nil {
		s.webSessions = make(map[string]string)
	}
	s.webSessions[token] = username
	return token, nil
}

func (s *Server) clearWebSession(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.webSessions, token)
}

func (s *Server) hasWebSession(r *http.Request) bool {
	if !s.webAuthEnabled() {
		return true
	}

	cookie, err := r.Cookie(webSessionCookieName)
	if err != nil || strings.TrimSpace(cookie.Value) == "" {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.webSessions[cookie.Value]
	return ok
}

func (s *Server) requireWebAuth(w http.ResponseWriter, r *http.Request) bool {
	if s.hasWebSession(r) {
		return true
	}
	if acceptsHTML(r) {
		http.Redirect(w, r, "/login", http.StatusFound)
		return false
	}
	http.Error(w, "unauthorized", http.StatusUnauthorized)
	return false
}

func (s *Server) handleWebLogin(w http.ResponseWriter, r *http.Request) {
	if !s.webAuthEnabled() {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(webLoginHTML))
	case http.MethodPost:
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		username := r.FormValue("username")
		password := r.FormValue("password")
		if !s.authenticateWebUser(username, password) {
			http.Error(w, "invalid username or password", http.StatusUnauthorized)
			return
		}
		token, err := s.createWebSession(username)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		http.SetCookie(w, &http.Cookie{
			Name:     webSessionCookieName,
			Value:    token,
			Path:     "/",
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
			Expires:  time.Now().Add(24 * time.Hour),
		})
		http.Redirect(w, r, "/", http.StatusFound)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleWebLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if cookie, err := r.Cookie(webSessionCookieName); err == nil {
		s.clearWebSession(cookie.Value)
	}
	http.SetCookie(w, &http.Cookie{
		Name:     webSessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		MaxAge:   -1,
		SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, "/login", http.StatusFound)
}

func acceptsHTML(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), "text/html")
}

const webLoginHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>LAN Share Login</title>
  <style>
    :root {
      --ink: #16201b;
      --muted: #5d6b63;
      --line: #cad6ce;
      --paper: #f4f0e8;
      --panel: #fffaf1;
      --accent: #176d4d;
      --accent-strong: #0d4f37;
      --shadow: 0 18px 50px rgba(31, 45, 38, 0.12);
    }
    * { box-sizing: border-box; }
    body {
      min-height: 100vh;
      margin: 0;
      display: grid;
      place-items: center;
      color: var(--ink);
      font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
      background:
        linear-gradient(135deg, rgba(23, 109, 77, 0.12), transparent 42%),
        repeating-linear-gradient(90deg, rgba(22, 32, 27, 0.035) 0 1px, transparent 1px 48px),
        var(--paper);
    }
    main {
      width: min(420px, calc(100vw - 28px));
      padding: 24px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: rgba(255, 250, 241, 0.95);
      box-shadow: var(--shadow);
    }
    h1 {
      margin: 0 0 8px;
      font-size: 30px;
      letter-spacing: 0;
    }
    p {
      margin: 0 0 18px;
      color: var(--muted);
      line-height: 1.5;
    }
    form {
      display: grid;
      gap: 12px;
    }
    label {
      display: grid;
      gap: 6px;
      color: var(--muted);
      font-size: 13px;
      font-weight: 700;
    }
    input {
      width: 100%;
      min-height: 42px;
      padding: 0 12px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #fffdf8;
      color: var(--ink);
      font: inherit;
    }
    button {
      min-height: 42px;
      border: 0;
      border-radius: 6px;
      background: var(--accent);
      color: #fff;
      font-weight: 800;
      cursor: pointer;
    }
    button:hover { background: var(--accent-strong); }
  </style>
</head>
<body>
  <main>
    <h1>LAN Share</h1>
    <p>Sign in to manage browser-based sharing on this tracker.</p>
    <form method="post" action="/login">
      <label>Username
        <input name="username" autocomplete="username" required>
      </label>
      <label>Password
        <input name="password" type="password" autocomplete="current-password" required>
      </label>
      <button type="submit">Sign in</button>
    </form>
  </main>
</body>
</html>`
