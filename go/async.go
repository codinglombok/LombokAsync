// Package lombokasync provides lightweight async utilities for Go.
//
// Uses goroutines and channels with an ergonomic API for task spawning,
// timers, channels, and combinators.
package lombokasync

import (
	"context"
	"sync"
	"time"
)

// JoinHandle represents a spawned task whose result can be awaited.
type JoinHandle[T any] struct {
	ch chan T
}

// Await blocks until the task completes and returns its result.
func (h *JoinHandle[T]) Await() T {
	return <-h.ch
}

// Spawn launches a function in a new goroutine and returns a JoinHandle.
func Spawn[T any](fn func() T) *JoinHandle[T] {
	ch := make(chan T, 1)
	go func() {
		ch <- fn()
	}()
	return &JoinHandle[T]{ch: ch}
}

// Sleep pauses the current goroutine for the given duration.
func Sleep(d time.Duration) {
	time.Sleep(d)
}

// Timeout runs fn with a deadline. Returns (result, true) if fn completes
// in time, or (zero, false) if the deadline elapses.
func Timeout[T any](d time.Duration, fn func() T) (T, bool) {
	ch := make(chan T, 1)
	go func() {
		ch <- fn()
	}()
	select {
	case val := <-ch:
		return val, true
	case <-time.After(d):
		var zero T
		return zero, false
	}
}

// TimeoutCtx runs fn with a context deadline.
func TimeoutCtx[T any](ctx context.Context, d time.Duration, fn func(context.Context) T) (T, bool) {
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	ch := make(chan T, 1)
	go func() {
		ch <- fn(ctx)
	}()
	select {
	case val := <-ch:
		return val, true
	case <-ctx.Done():
		var zero T
		return zero, false
	}
}

// --- Channels ---

// MpscSender is the sending half of an mpsc channel.
type MpscSender[T any] struct {
	ch chan T
}

// Send sends a value into the channel.
func (s *MpscSender[T]) Send(value T) {
	s.ch <- value
}

// Close closes the channel.
func (s *MpscSender[T]) Close() {
	close(s.ch)
}

// MpscReceiver is the receiving half of an mpsc channel.
type MpscReceiver[T any] struct {
	ch chan T
}

// Recv receives the next value. Returns (value, true) or (zero, false) if closed.
func (r *MpscReceiver[T]) Recv() (T, bool) {
	val, ok := <-r.ch
	return val, ok
}

// MpscChannel creates an unbounded-ish mpsc channel with the given buffer size.
func MpscChannel[T any](bufSize int) (*MpscSender[T], *MpscReceiver[T]) {
	ch := make(chan T, bufSize)
	return &MpscSender[T]{ch: ch}, &MpscReceiver[T]{ch: ch}
}

// OneshotSender sends exactly one value.
type OneshotSender[T any] struct {
	ch chan T
}

// Send sends a value and closes the channel.
func (s *OneshotSender[T]) Send(value T) {
	s.ch <- value
	close(s.ch)
}

// OneshotReceiver receives exactly one value.
type OneshotReceiver[T any] struct {
	ch chan T
}

// Recv waits for and returns the value.
func (r *OneshotReceiver[T]) Recv() T {
	return <-r.ch
}

// OneshotChannel creates a oneshot channel.
func OneshotChannel[T any]() (*OneshotSender[T], *OneshotReceiver[T]) {
	ch := make(chan T, 1)
	return &OneshotSender[T]{ch: ch}, &OneshotReceiver[T]{ch: ch}
}

// --- Combinators ---

// Join runs two functions concurrently and returns both results.
func Join[A, B any](fa func() A, fb func() B) (A, B) {
	var a A
	var b B
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		a = fa()
	}()
	go func() {
		defer wg.Done()
		b = fb()
	}()
	wg.Wait()
	return a, b
}

// Join3 runs three functions concurrently and returns all results.
func Join3[A, B, C any](fa func() A, fb func() B, fc func() C) (A, B, C) {
	var a A
	var b B
	var c C
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		a = fa()
	}()
	go func() {
		defer wg.Done()
		b = fb()
	}()
	go func() {
		defer wg.Done()
		c = fc()
	}()
	wg.Wait()
	return a, b, c
}

// JoinAll runs a slice of functions concurrently and returns all results in order.
func JoinAll[T any](fns []func() T) []T {
	results := make([]T, len(fns))
	var wg sync.WaitGroup
	wg.Add(len(fns))
	for i, fn := range fns {
		go func(idx int, f func() T) {
			defer wg.Done()
			results[idx] = f()
		}(i, fn)
	}
	wg.Wait()
	return results
}

// Either represents the result of a select operation.
type Either[A, B any] struct {
	IsLeft bool
	Left   A
	Right  B
}

// Select races two functions — returns whichever completes first.
func Select[A, B any](fa func() A, fb func() B) Either[A, B] {
	type resultA struct{ val A }
	type resultB struct{ val B }

	chA := make(chan resultA, 1)
	chB := make(chan resultB, 1)

	go func() { chA <- resultA{fa()} }()
	go func() { chB <- resultB{fb()} }()

	select {
	case ra := <-chA:
		return Either[A, B]{IsLeft: true, Left: ra.val}
	case rb := <-chB:
		return Either[A, B]{IsLeft: false, Right: rb.val}
	}
}

// Interval returns a channel that receives tick indices at regular intervals.
// Close the returned stop channel to stop the ticker.
func Interval(d time.Duration) (<-chan int, chan<- struct{}) {
	ch := make(chan int)
	stop := make(chan struct{})
	go func() {
		defer close(ch)
		count := 0
		ticker := time.NewTicker(d)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ch <- count
				count++
			case <-stop:
				return
			}
		}
	}()
	return ch, stop
}
