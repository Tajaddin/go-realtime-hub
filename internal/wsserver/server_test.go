package wsserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Tajaddin/go-realtime-hub/internal/hub"
	"github.com/gorilla/websocket"
)

func startServer(t *testing.T) (string, func()) {
	t.Helper()
	srv := httptest.NewServer(New(hub.New()).Handler())
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	return wsURL, srv.Close
}

func dial(t *testing.T, wsURL, room, id string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial(wsURL+"/ws?room="+room+"&id="+id, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", id, err)
	}
	return conn
}

func TestWebSocketBroadcastBetweenClients(t *testing.T) {
	wsURL, closeSrv := startServer(t)
	defer closeSrv()

	alice := dial(t, wsURL, "lobby", "alice")
	defer alice.Close()
	bob := dial(t, wsURL, "lobby", "bob")
	defer bob.Close()

	// Give both joins time to register before publishing.
	time.Sleep(100 * time.Millisecond)

	if err := alice.WriteMessage(websocket.TextMessage, []byte("hello lobby")); err != nil {
		t.Fatalf("write: %v", err)
	}

	// bob receives alice's message
	_ = bob.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg, err := bob.ReadMessage()
	if err != nil {
		t.Fatalf("bob read: %v", err)
	}
	if string(msg) != "hello lobby" {
		t.Fatalf("bob got %q", msg)
	}
}

func TestPresenceEndpoint(t *testing.T) {
	wsURL, closeSrv := startServer(t)
	defer closeSrv()
	httpBase := "http" + strings.TrimPrefix(wsURL, "ws")

	c := dial(t, wsURL, "team", "alice")
	defer c.Close()
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get(httpBase + "/presence?room=team")
	if err != nil {
		t.Fatalf("presence get: %v", err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 256)
	n, _ := resp.Body.Read(buf)
	if !strings.Contains(string(buf[:n]), "alice") {
		t.Fatalf("presence body missing alice: %s", buf[:n])
	}
}

func TestHealthz(t *testing.T) {
	wsURL, closeSrv := startServer(t)
	defer closeSrv()
	httpBase := "http" + strings.TrimPrefix(wsURL, "ws")
	resp, err := http.Get(httpBase + "/healthz")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("healthz failed: %v status=%v", err, resp.StatusCode)
	}
}

func TestRequiresRoom(t *testing.T) {
	wsURL, closeSrv := startServer(t)
	defer closeSrv()
	_, _, err := websocket.DefaultDialer.Dial(wsURL+"/ws", nil)
	if err == nil {
		t.Fatal("expected dial without room to fail")
	}
}
