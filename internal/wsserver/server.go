// Package wsserver bridges WebSocket connections to the hub. Each connection
// gets a read pump (incoming messages are published to its room) and a write
// pump (drains the hub send channel to the socket).
package wsserver

import (
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Tajaddin/go-realtime-hub/internal/hub"
	"github.com/gorilla/websocket"
)

var idSeq atomic.Int64

type Server struct {
	hub            *hub.Hub
	upgrader       websocket.Upgrader
	sendBuf        int
	allowedOrigins map[string]bool
}

// New returns a server for h. A browser may open a WebSocket only from the
// server's own origin or from one of allowedOrigins (for example
// "https://app.example.com"); clients that send no Origin header, which
// browsers always do, are not restricted.
func New(h *hub.Hub, allowedOrigins ...string) *Server {
	s := &Server{
		hub:            h,
		sendBuf:        64,
		allowedOrigins: make(map[string]bool, len(allowedOrigins)),
	}
	for _, o := range allowedOrigins {
		s.allowedOrigins[strings.ToLower(o)] = true
	}
	s.upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     s.checkOrigin,
	}
	return s
}

func (s *Server) checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" || s.allowedOrigins[strings.ToLower(origin)] {
		return true
	}
	u, err := url.Parse(origin)
	return err == nil && strings.EqualFold(u.Host, r.Host)
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(s.hub.Stats()); err != nil {
			log.Printf("encode /stats: %v", err)
		}
	})
	mux.HandleFunc("GET /presence", func(w http.ResponseWriter, r *http.Request) {
		room := r.URL.Query().Get("room")
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"room": room, "members": s.hub.Presence(room)}); err != nil {
			log.Printf("encode /presence: %v", err)
		}
	})
	mux.HandleFunc("GET /ws", s.serveWS)
	return mux
}

func (s *Server) serveWS(w http.ResponseWriter, r *http.Request) {
	room := r.URL.Query().Get("room")
	if room == "" {
		http.Error(w, "room query param required", http.StatusBadRequest)
		return
	}
	id := r.URL.Query().Get("id")
	if id == "" {
		id = "c" + time.Now().Format("150405.000") + "-" + itoa(idSeq.Add(1))
	}

	client, err := s.hub.AddClient(id, s.sendBuf)
	if err != nil {
		http.Error(w, "client id already connected", http.StatusConflict)
		return
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.hub.RemoveClient(id)
		return // upgrader already wrote the error
	}

	s.hub.Join(id, room)
	defer func() {
		s.hub.RemoveClient(id)
		if err := conn.Close(); err != nil {
			log.Printf("ws close (id=%s): %v", id, err)
		}
	}()

	go s.writePump(conn, client)
	s.readPump(conn, id, room)
}

func (s *Server) readPump(conn *websocket.Conn, id, room string) {
	conn.SetReadLimit(1 << 20)
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		s.hub.Publish(room, msg)
	}
}

func (s *Server) writePump(conn *websocket.Conn, client *hub.Client) {
	for msg := range client.Send {
		if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			return
		}
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
