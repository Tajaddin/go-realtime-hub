package hub

import (
	"fmt"
	"sync"
	"testing"
)

func drain(c *Client) [][]byte {
	var out [][]byte
	for {
		select {
		case m := <-c.Send:
			out = append(out, m)
		default:
			return out
		}
	}
}

func TestPublishFansOutToAllMembers(t *testing.T) {
	h := New()
	a := h.AddClient("a", 8)
	b := h.AddClient("b", 8)
	h.Join("a", "room1")
	h.Join("b", "room1")

	n := h.Publish("room1", []byte("hi"))
	if n != 2 {
		t.Fatalf("delivered=%d, want 2", n)
	}
	if len(drain(a)) != 1 || len(drain(b)) != 1 {
		t.Fatal("both members should receive one message")
	}
}

func TestPublishOnlyToRoom(t *testing.T) {
	h := New()
	a := h.AddClient("a", 8)
	b := h.AddClient("b", 8)
	h.Join("a", "room1")
	h.Join("b", "room2")
	h.Publish("room1", []byte("x"))
	if len(drain(a)) != 1 || len(drain(b)) != 0 {
		t.Fatal("only room1 members should receive")
	}
}

func TestPresenceSorted(t *testing.T) {
	h := New()
	for _, id := range []string{"carol", "alice", "bob"} {
		h.AddClient(id, 4)
		h.Join(id, "r")
	}
	got := h.Presence("r")
	want := []string{"alice", "bob", "carol"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("presence=%v, want %v", got, want)
	}
}

func TestLeaveStopsDelivery(t *testing.T) {
	h := New()
	h.AddClient("a", 4)
	h.Join("a", "r")
	h.Leave("a", "r")
	if h.Publish("r", []byte("x")) != 0 {
		t.Fatal("left client should not receive")
	}
	if h.RoomSize("r") != 0 {
		t.Fatal("empty room should be cleaned up")
	}
}

func TestRemoveClientCleansRooms(t *testing.T) {
	h := New()
	h.AddClient("a", 4)
	h.Join("a", "r1")
	h.Join("a", "r2")
	h.RemoveClient("a")
	if h.ClientCount() != 0 || h.RoomSize("r1") != 0 || h.RoomSize("r2") != 0 {
		t.Fatal("removing a client must clear it from all rooms")
	}
}

func TestBackpressureDropsInsteadOfBlocking(t *testing.T) {
	h := New()
	// buffer of 2; never drained. The 3rd+ publishes must drop, not block.
	h.AddClient("slow", 2)
	h.Join("slow", "r")
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			h.Publish("r", []byte("x"))
		}
		close(done)
	}()
	<-done // would deadlock if Publish blocked on the full buffer
	s := h.Stats()
	if s.Delivered != 2 {
		t.Fatalf("delivered=%d, want 2 (buffer size)", s.Delivered)
	}
	if s.Dropped != 98 {
		t.Fatalf("dropped=%d, want 98", s.Dropped)
	}
}

func TestConcurrentJoinPublishIsRaceFree(t *testing.T) {
	h := New()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("c%d", i)
			c := h.AddClient(id, 256)
			h.Join(id, "r")
			go func() { drain(c) }()
			for j := 0; j < 100; j++ {
				h.Publish("r", []byte("m"))
			}
		}(i)
	}
	wg.Wait()
	if h.ClientCount() != 50 {
		t.Fatalf("clients=%d, want 50", h.ClientCount())
	}
}
