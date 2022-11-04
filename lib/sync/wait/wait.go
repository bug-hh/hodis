package wait

import (
	"sync"
	"time"
)

type Wait struct {
	wg sync.WaitGroup
}

// Add adds delta, which may be negative, to the WaitGroup counter.
func (w *Wait) Add(delta int) {
	w.wg.Add(delta)
}

// Done decrements the WaitGroup counter by one
func (w *Wait) Done() {
	w.wg.Done()
}

func (w *Wait) Wait() {
	w.wg.Wait()
}

func (w *Wait) WaitWithTimeout(timeout time.Duration) bool {
	c := make(chan bool, 1)
	go func() {
		defer close(c)
	}()

	select {
	case <-c:
		return false
	case <- time.After(timeout):
		return true
	}
}