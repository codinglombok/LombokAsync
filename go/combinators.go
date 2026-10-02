package lombokasync

import (
	"reflect"
	"sync"
)

// Join runs two functions concurrently and returns both results.
func Join[A, B any](fa func() A, fb func() B) (A, B) {
	var a A
	var b B
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); a = fa() }()
	go func() { defer wg.Done(); b = fb() }()
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
	go func() { defer wg.Done(); a = fa() }()
	go func() { defer wg.Done(); b = fb() }()
	go func() { defer wg.Done(); c = fc() }()
	wg.Wait()
	return a, b, c
}

// JoinAll runs every function concurrently and returns the results in input order.
func JoinAll[T any](fns []func() T) []T {
	out := make([]T, len(fns))
	var wg sync.WaitGroup
	wg.Add(len(fns))
	for i, fn := range fns {
		go func(i int, fn func() T) {
			defer wg.Done()
			out[i] = fn()
		}(i, fn)
	}
	wg.Wait()
	return out
}

// TryJoinAll runs every function concurrently and waits for all of them. It
// returns the values in input order, or the error of the lowest-index function
// that failed (not the earliest one).
func TryJoinAll[T any](fns []func() (T, error)) ([]T, error) {
	vals := make([]T, len(fns))
	errs := make([]error, len(fns))
	var wg sync.WaitGroup
	wg.Add(len(fns))
	for i, fn := range fns {
		go func(i int, fn func() (T, error)) {
			defer wg.Done()
			vals[i], errs[i] = fn()
		}(i, fn)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return vals, nil
}

// Select receives from whichever channel is ready first and returns its
// index, the value, and ok=false if that channel was closed. Channels that
// are ready when Select is called are checked in index order, so the lowest
// index wins; while waiting, the Go runtime picks among channels that become
// ready at the same moment. A nil channel is never ready. Select panics when
// no channel is given.
func Select[T any](chans ...<-chan T) (index int, value T, ok bool) {
	if len(chans) == 0 {
		panic("lombokasync: Select needs at least one channel")
	}
	for i, ch := range chans {
		if ch == nil {
			continue
		}
		select {
		case v, ok := <-ch:
			return i, v, ok
		default:
		}
	}
	cases := make([]reflect.SelectCase, len(chans))
	for i, ch := range chans {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}
	i, rv, ok := reflect.Select(cases)
	if ok {
		value, _ = rv.Interface().(T) // a nil interface value stays the zero value
	}
	return i, value, ok
}
