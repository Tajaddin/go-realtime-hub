// Package hub is a concurrency-safe pub/sub core for a realtime server:
// clients join rooms, messages published to a room fan out to every member.
//
// The hot path is Publish. It does a non-blocking send to each subscriber, so
// one slow client can never stall the publisher or the other subscribers: if a
// client's buffer is full its message is dropped and counted. This bounded,
// lossy-for-slow-clients fan-out is what keeps a realtime server responsive
// under load. The core has no WebSocket dependency, so it is unit-tested and
// benchmarked directly.
package hub

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
)

// Client is a subscriber. Send is drained by the owner (a WebSocket writer in
// production, a test goroutine in tests).
type Client struct {
	ID    string
	Send  chan []byte
	rooms map[string]struct{}
}

type Hub struct {
	mu      sync.RWMutex
	clients map[string]*Client
	rooms   map[string]map[string]*Client // room -> clientID -> client

	delivered atomic.Int64
	dropped   atomic.Int64
}

func New() *Hub {
	return &Hub{
		clients: make(map[string]*Client),
		rooms:   make(map[string]map[string]*Client),
	}
}

// ErrClientExists is returned by AddClient when the id is already registered.
var ErrClientExists = errors.New("hub: client id already registered")

// AddClient registers a client with a send buffer of bufSize. An id that is
// already registered is rejected with ErrClientExists, so a second caller can
// never share or take over another client's message stream.
func (h *Hub) AddClient(id string, bufSize int) (*Client, error) {
	if bufSize <= 0 {
		bufSize = 16
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[id]; ok {
		return nil, ErrClientExists
	}
	c := &Client{ID: id, Send: make(chan []byte, bufSize), rooms: make(map[string]struct{})}
	h.clients[id] = c
	return c, nil
}

// RemoveClient unsubscribes a client from all rooms and drops it.
func (h *Hub) RemoveClient(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.clients[id]
	if !ok {
		return
	}
	for room := range c.rooms {
		if members := h.rooms[room]; members != nil {
			delete(members, id)
			if len(members) == 0 {
				delete(h.rooms, room)
			}
		}
	}
	delete(h.clients, id)
	close(c.Send)
}

func (h *Hub) Join(clientID, room string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.clients[clientID]
	if !ok {
		return false
	}
	members := h.rooms[room]
	if members == nil {
		members = make(map[string]*Client)
		h.rooms[room] = members
	}
	members[clientID] = c
	c.rooms[room] = struct{}{}
	return true
}

func (h *Hub) Leave(clientID, room string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if c, ok := h.clients[clientID]; ok {
		delete(c.rooms, room)
	}
	if members := h.rooms[room]; members != nil {
		delete(members, clientID)
		if len(members) == 0 {
			delete(h.rooms, room)
		}
	}
}

// Publish fans a message out to every member of room. Returns how many
// subscribers received it. A full subscriber buffer means a dropped message
// (counted), never a blocked publisher.
//
// The read lock is held for the whole fan-out so RemoveClient cannot close a
// member's Send channel between the lookup and the send. Every send is
// non-blocking, so the lock is never held waiting on a subscriber.
func (h *Hub) Publish(room string, msg []byte) (deliveredCount int) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, c := range h.rooms[room] {
		select {
		case c.Send <- msg:
			deliveredCount++
			h.delivered.Add(1)
		default:
			h.dropped.Add(1)
		}
	}
	return deliveredCount
}

// Presence returns the sorted client ids currently in room.
func (h *Hub) Presence(room string) []string {
	h.mu.RLock()
	members := h.rooms[room]
	ids := make([]string, 0, len(members))
	for id := range members {
		ids = append(ids, id)
	}
	h.mu.RUnlock()
	sort.Strings(ids)
	return ids
}

func (h *Hub) RoomSize(room string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.rooms[room])
}

func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

type Stats struct {
	Clients   int   `json:"clients"`
	Rooms     int   `json:"rooms"`
	Delivered int64 `json:"delivered"`
	Dropped   int64 `json:"dropped"`
}

func (h *Hub) Stats() Stats {
	h.mu.RLock()
	clients := len(h.clients)
	rooms := len(h.rooms)
	h.mu.RUnlock()
	return Stats{Clients: clients, Rooms: rooms, Delivered: h.delivered.Load(), Dropped: h.dropped.Load()}
}
