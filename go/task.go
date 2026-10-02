package lombokasync

import (
	"context"
	"sync"
	"time"
)

// JoinHandle is the result of a function started with Spawn.
type JoinHandle[T any] struct {
	done   chan struct{}
	val    T
	panicV any
}

// Spawn runs fn in a new goroutine and returns a handle to its result.
// A panic in fn is caught and raised again by Await.
func Spawn[T any](fn func() T) *JoinHandle[T] {
	h := &JoinHandle[T]{done: make(chan struct{})}
	go func() {
		defer close(h.done)
		defer func() {
			if r := recover(); r != nil {
				h.panicV = r
			}
		}()
		h.val = fn()
	}()
	return h
}

// Await blocks until the function returns and gives its result. It may be
// called any number of times.
func (h *JoinHandle[T]) Await() T {
	<-h.done
	if h.panicV != nil {
		panic(h.panicV)
	}
	return h.val
}

// Done returns a channel that is closed when the function has returned.
func (h *JoinHandle[T]) Done() <-chan struct{} { return h.done }

// Sleep pauses the current goroutine for d.
func Sleep(d time.Duration) { time.Sleep(d) }

// Timeout runs fn in a goroutine and returns its result, or ErrTimeout when d
// elapses first. Go cannot stop a goroutine from outside: on timeout fn keeps
// running until it returns. Use TimeoutCtx for work that can be cancelled.
func Timeout[T any](d time.Duration, fn func() T) (T, error) {
	return TimeoutCtx(context.Background(), d, func(context.Context) T { return fn() })
}

// TimeoutCtx is Timeout with a context that is cancelled when the deadline
// elapses or the parent is done, so fn can stop early. A result that is ready
// when the deadline fires still wins.
func TimeoutCtx[T any](parent context.Context, d time.Duration, fn func(context.Context) T) (T, error) {
	ctx, cancel := context.WithTimeout(parent, d)
	defer cancel()
	ch := make(chan T, 1)
	go func() { ch <- fn(ctx) }()
	select {
	case v := <-ch:
		return v, nil
	case <-ctx.Done():
		select {
		case v := <-ch:
			return v, nil
		default:
		}
		var zero T
		if parent.Err() != nil {
			return zero, parent.Err()
		}
		return zero, ErrTimeout
	}
}

// TimeoutErr is Timeout for functions that can fail; their error passes through.
func TimeoutErr[T any](d time.Duration, fn func() (T, error)) (T, error) {
	type res struct {
		v   T
		err error
	}
	r, err := Timeout(d, func() res {
		v, e := fn()
		return res{v, e}
	})
	if err != nil {
		return r.v, err
	}
	return r.v, r.err
}

// Interval sends 0, 1, 2, ... on the returned channel every d until stop is
// called. Ticks that the receiver is too slow to take are dropped, as with
// time.Ticker. It panics when d <= 0.
func Interval(d time.Duration) (ticks <-chan int, stop func()) {
	if d <= 0 {
		panic("lombokasync: interval period must be greater than zero")
	}
	ch := make(chan int)
	quit := make(chan struct{})
	t := time.NewTicker(d)
	go func() {
		defer t.Stop()
		defer close(ch)
		for n := 0; ; n++ {
			select {
			case <-t.C:
			case <-quit:
				return
			}
			select {
			case ch <- n:
			case <-quit:
				return
			}
		}
	}()
	var once sync.Once
	return ch, func() { once.Do(func() { close(quit) }) }
}
