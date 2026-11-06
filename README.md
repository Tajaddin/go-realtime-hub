# go-realtime-hub

> Realtime WebSocket hub in Go: clients join rooms, messages fan out to every member, with presence and backpressure. **~1.55M message deliveries/sec (median of 14 runs)** fanning out to 10,000 subscribers in one room, 0 dropped. Non-blocking fan-out (a slow client never stalls the publisher), 18 tests under the race detector, distroless image.

[![ci](https://github.com/Tajaddin/go-realtime-hub/actions/workflows/ci.yml/badge.svg)](https://github.com/Tajaddin/go-realtime-hub/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.23-00ADD8)](go.mod)

## Hero metrics

Reproducible in-process (no network), so it isolates the pub/sub core:

```bash
go run ./load -subscribers 10000 -messages 200
```

| Metric | Value |
|---|---:|
| Subscribers in the room | 10,000 |
| Messages published | 200 |
| Fan-out operations | 2,000,000 |
| **Delivery throughput (median of 14 runs)** | **~1,549,000 /sec** |
| Slowest / fastest run | 1,057,842 / 6,470,314 /sec |
| Dropped (consumers kept up) | 0 |

Measured 2026-11-06 on an Intel i5-13600KF (14 cores / 20 threads, 32 GB, Windows 11, Go 1.27.3) on a machine shared with other jobs (total CPU 68-90% when sampled before and after the runs), so this is a loaded-machine figure, not an idle one. Throughput swings with machine load because the publisher and 10,000 consumer goroutines compete for CPU, so read the median, not the fastest run. The earlier ~1.07M figure was measured on a different machine (an i7-10875H), so the rise from it reflects the hardware, not a code change: on this machine the previous code, run alternately with the current one, had a median of ~1.82M, a gap smaller than the run-to-run spread. See [`bench/results.txt`](bench/results.txt) for every run.

The Publish hot path does a **non-blocking** send to each subscriber. A client whose buffer is full has its message dropped and counted, so one slow consumer can never stall the publisher or the other 9,999 subscribers. That bounded, lossy-for-slow-clients fan-out is what keeps a realtime server responsive under load.

## What it is

```
client --ws--> /ws?room=lobby ──┐
                                ▼
                         hub (rooms, presence)
                                │  Publish fans out
                                ▼
        every member's send buffer (sender included) --ws--> client
```

| Concern | Implementation |
|---|---|
| Transport | WebSocket via `gorilla/websocket`, one read pump + one write pump per connection; ping every 54 s, a peer with no pong for 60 s is dropped, and a write that fails or takes over 10 s closes the connection |
| Pub/sub core | `internal/hub`: `RWMutex`-guarded rooms, non-blocking fan-out, drop-counting backpressure |
| Presence | sorted member list per room, exposed at `/presence?room=` |
| Backpressure | per-client buffered channel; full buffer drops, never blocks (proven in tests) |
| Safety | race-detector-clean under 50-goroutine concurrent join/publish and under publish during client removal; duplicate client ids refused; browser origins allowlisted |
| Deploy | static `CGO_ENABLED=0` binary in a distroless nonroot image |

## Why this matters for hiring

Role categories unlocked: **Backend (Go)**, realtime/streaming, Platform.

Realtime fan-out (chat, presence, live dashboards, multiplayer) is a common modern requirement, and getting backpressure right is the hard part most implementations miss. This backs the "WebSocket / realtime / Go concurrency" resume line with a measured ~1.55M-delivery/sec core and a race-tested concurrency model.

## How to run

Prerequisites: Go 1.23+ (Docker optional for the distroless image).

```bash
go test ./...                # 18 unit + integration tests
go run ./cmd/server          # ws://localhost:8080/ws?room=lobby
# browsers may connect only from the server's own origin unless you allow others:
go run ./cmd/server -allowed-origins https://app.example.com
go run ./load -subscribers 10000 -messages 200   # fan-out benchmark
# alt: distroless container
docker build -t go-realtime-hub . && docker run -p 8080:8080 go-realtime-hub
```

```bash
# connect (e.g. with websocat) two clients to the same room; a message from one
# is delivered to the other:
websocat "ws://localhost:8080/ws?room=lobby&id=alice"
websocat "ws://localhost:8080/ws?room=lobby&id=bob"
# `id` is optional (the server generates one); an id that is already connected
# is refused with 409 Conflict, so nobody can take over another client's stream.
# A client that drops without closing its socket and reconnects with the same
# id is refused until the server notices the old connection is dead (up to
# 60 s); omit `id` on reconnect to avoid the wait.

curl localhost:8080/presence?room=lobby   # {"room":"lobby","members":["alice","bob"]}
curl localhost:8080/stats                  # {"clients":2,"rooms":1,"delivered":N,"dropped":0}
```

## Testing

```bash
go test ./... -race        # 18 tests, race detector on
go test ./internal/... -cover
```

- **hub**: fan-out to all members, room isolation, sorted presence, leave stops delivery, remove-client cleans all rooms, **backpressure drops instead of blocking**, 50-goroutine concurrent join/publish race-freedom, publish during client removal, duplicate-id rejection (95.7% coverage).
- **wsserver**: real WebSocket broadcast between two `gorilla` clients over `httptest`, presence endpoint, healthz, room-required rejection, duplicate id refused with 409, origin allowlist, unresponsive peer dropped and live peer kept by ping/pong, peer with stalled writes dropped (77.8% coverage).

## Project layout

```
internal/hub/hub.go        # rooms + presence + non-blocking fan-out (the core)
internal/wsserver/server.go# gorilla WebSocket bridge (read/write pumps) + HTTP endpoints
cmd/server/main.go         # runnable server
load/main.go               # in-process fan-out throughput benchmark
```

## Stack

Go 1.23, gorilla/websocket, net/http, distroless Docker, GitHub Actions (vet + race tests + coverage gate + image build).

## License

MIT
