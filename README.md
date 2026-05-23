# go-realtime-hub

> Realtime WebSocket hub in Go: clients join rooms, messages fan out to every member, with presence and backpressure. **~1.07M message deliveries/sec median (commodity load), 1.28M peak (idle machine)** fanning out to 10,000 subscribers in one room, 0 dropped. Lock-free-for-slow-clients fan-out, 12 tests under the race detector, distroless image.

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
| **Delivery throughput (median, commodity load)** | **~1,066,000 /sec** |
| **Delivery throughput (peak, idle machine)** | **1,281,987 /sec** |
| Dropped (consumers kept up) | 0 |

Throughput depends on machine load: both the publisher and 10,000 consumer goroutines compete for CPU. Today's 3-run median on an i7-10875H under normal dev-session load: 1,088,989 / 1,066,319 / 926,731 deliveries/sec (median 1.07M, max 1.09M). The 1.28M peak was captured on the same hardware with the machine idle. See [`bench/results.txt`](bench/results.txt) for both runs and full hardware specs.

The Publish hot path does a **non-blocking** send to each subscriber. A client whose buffer is full has its message dropped and counted, so one slow consumer can never stall the publisher or the other 9,999 subscribers. That bounded, lossy-for-slow-clients fan-out is what keeps a realtime server responsive under load.

## What it is

```
client --ws--> /ws?room=lobby ──┐
                                ▼
                         hub (rooms, presence)
                                │  Publish fans out
                                ▼
            every other member's send buffer --ws--> client
```

| Concern | Implementation |
|---|---|
| Transport | WebSocket via `gorilla/websocket`, one read pump + one write pump per connection |
| Pub/sub core | `internal/hub`: `RWMutex`-guarded rooms, non-blocking fan-out, drop-counting backpressure |
| Presence | sorted member list per room, exposed at `/presence?room=` |
| Backpressure | per-client buffered channel; full buffer drops, never blocks (proven in tests) |
| Safety | race-detector-clean under 50-goroutine concurrent join/publish |
| Deploy | static `CGO_ENABLED=0` binary in a distroless nonroot image |

## Why this matters for hiring

Role categories unlocked: **Backend (Go)**, realtime/streaming, Platform.

Realtime fan-out (chat, presence, live dashboards, multiplayer) is a common modern requirement, and getting backpressure right is the hard part most implementations miss. This backs the "WebSocket / realtime / Go concurrency" resume line with a measured 1.28M-delivery/sec core and a race-tested concurrency model.

## How to run

Prerequisites: Go 1.23+ (Docker optional for the distroless image).

```bash
go test ./...                # 12 race-tested unit + integration cases
go run ./cmd/server          # ws://localhost:8080/ws?room=lobby
go run ./load -subscribers 10000 -messages 200   # fan-out benchmark
# alt: distroless container
docker build -t go-realtime-hub . && docker run -p 8080:8080 go-realtime-hub
```

```bash
# connect (e.g. with websocat) two clients to the same room; a message from one
# is delivered to the other:
websocat "ws://localhost:8080/ws?room=lobby&id=alice"
websocat "ws://localhost:8080/ws?room=lobby&id=bob"

curl localhost:8080/presence?room=lobby   # {"room":"lobby","members":["alice","bob"]}
curl localhost:8080/stats                  # {"clients":2,"rooms":1,"delivered":N,"dropped":0}
```

## Testing

```bash
go test ./... -race        # 12 tests, race detector on
go test ./internal/... -cover
```

- **hub**: fan-out to all members, room isolation, sorted presence, leave stops delivery, remove-client cleans all rooms, **backpressure drops instead of blocking**, 50-goroutine concurrent join/publish race-freedom (94.5% coverage).
- **wsserver**: real WebSocket broadcast between two `gorilla` clients over `httptest`, presence endpoint, healthz, room-required rejection (71.4% coverage).

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
