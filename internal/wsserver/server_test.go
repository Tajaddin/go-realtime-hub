package wsserver

import (
	"fmt"
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

func TestDuplicateIDIsRejected(t *testing.T) {
	wsURL, closeSrv := startServer(t)
	defer closeSrv()

	alice := dial(t, wsURL, "lobby", "alice")
	defer alice.Close()
	bob := dial(t, wsURL, "lobby", "bob")
	defer bob.Close()
	time.Sleep(100 * time.Millisecond)

	imposter, resp, err := websocket.DefaultDialer.Dial(wsURL+"/ws?room=lobby&id=alice", nil)
	if err == nil {
		imposter.Close()
		t.Fatal("second connection with a connected client's id was accepted")
	}
	if resp == nil || resp.StatusCode != http.StatusConflict {
		t.Fatalf("want 409 Conflict for a duplicate id, got %v", resp)
	}

	if err := bob.WriteMessage(websocket.TextMessage, []byte("still there?")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = alice.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, msg, err := alice.ReadMessage(); err != nil || string(msg) != "still there?" {
		t.Fatalf("original alice should keep receiving: msg=%q err=%v", msg, err)
	}
}

func TestCheckOrigin(t *testing.T) {
	srv := httptest.NewServer(New(hub.New(), "https://app.example").Handler())
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	for i, tc := range []struct {
		origin string
		want   int
	}{
		{"https://evil.example", http.StatusForbidden},
		{"https://app.example", http.StatusSwitchingProtocols},
		{srv.URL, http.StatusSwitchingProtocols}, // same origin
		{"", http.StatusSwitchingProtocols},      // non-browser client
	} {
		hdr := http.Header{}
		if tc.origin != "" {
			hdr.Set("Origin", tc.origin)
		}
		conn, resp, err := websocket.DefaultDialer.Dial(fmt.Sprintf("%s/ws?room=r&id=c%d", wsURL, i), hdr)
		if conn != nil {
			conn.Close()
		}
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		if status != tc.want {
			t.Errorf("Origin %q: status %d (err %v), want %d", tc.origin, status, err, tc.want)
		}
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
