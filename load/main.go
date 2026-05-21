// Command load benchmarks the hub's fan-out: one room, many subscribers, a
// stream of published messages. It measures total deliveries per second and
// the drop rate under a bounded per-client buffer. In-process (no network), so
// it isolates the pub/sub core's throughput.
//
//	go run ./load -subscribers 10000 -messages 200
package main

import (
	"flag"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Tajaddin/go-realtime-hub/internal/hub"
)

func main() {
	subs := flag.Int("subscribers", 10000, "subscribers in the room")
	messages := flag.Int("messages", 200, "messages published")
	buf := flag.Int("buf", 64, "per-client send buffer")
	flag.Parse()

	h := hub.New()
	const room = "load"

	var received atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < *subs; i++ {
		c := h.AddClient(fmt.Sprintf("c%d", i), *buf)
		h.Join(c.ID, room)
		wg.Add(1)
		go func(ch <-chan []byte) {
			defer wg.Done()
			for {
				select {
				case _, ok := <-ch:
					if !ok {
						return
					}
					received.Add(1)
				case <-stop:
					// drain remaining without blocking
					for {
						select {
						case _, ok := <-ch:
							if !ok {
								return
							}
							received.Add(1)
						default:
							return
						}
					}
				}
			}
		}(c.Send)
	}

	msg := []byte(`{"type":"chat","body":"hello world"}`)
	start := time.Now()
	var totalDelivered int64
	for i := 0; i < *messages; i++ {
		totalDelivered += int64(h.Publish(room, msg))
	}
	elapsed := time.Since(start)
	close(stop)
	wg.Wait()

	stats := h.Stats()
	fanouts := int64(*subs) * int64(*messages)
	fmt.Printf("subscribers:        %d\n", *subs)
	fmt.Printf("messages:           %d\n", *messages)
	fmt.Printf("fan-out operations: %d\n", fanouts)
	fmt.Printf("publish_seconds:    %.4f\n", elapsed.Seconds())
	fmt.Printf("deliveries_per_sec: %.0f\n", float64(stats.Delivered)/elapsed.Seconds())
	fmt.Printf("delivered (sent):   %d\n", stats.Delivered)
	fmt.Printf("dropped (slow buf): %d\n", stats.Dropped)
	fmt.Printf("received (drained): %d\n", received.Load())
}
