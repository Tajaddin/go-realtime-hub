// Package wsserver bridges WebSocket connections to the hub. Each connection
// gets a read pump (incoming messages are published to its room) and a write
// pump (drains the hub send channel to the socket).
package wsserver

import (
	"encoding/json"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/Tajaddin/go-realtime-hub/internal/hub"
	"github.com/gorilla/websocket"
)

var idSeq atomic.Int64

type Server struct {
	hub      *hub.Hub
	upgrader websocket.Upgrader
	sendBuf  int
}

func New(h *hub.Hub) *Server {
	return &Server{
		hub:     h,
		sendBuf: 64,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			// Demo server: accept any origin. Lock this down in production.
			CheckOrigin: func(_ *http.Request) bool { return true },
		},
	}
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

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // upgrader already wrote the error
	}

	client := s.hub.AddClient(id, s.sendBuf)
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
