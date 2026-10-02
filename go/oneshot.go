package lombokasync

import (
	"context"
	"sync"
)

type oneshotState[T any] struct {
	mu        sync.Mutex
	value     T
	hasValue  bool
	txUsed    bool
	txDropped bool
	rxClosed  bool
	taken     bool
	changed   chan struct{}
}

func (s *oneshotState[T]) broadcast() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// OneshotSender sends at most one value.
type OneshotSender[T any] struct{ s *oneshotState[T] }

// OneshotReceiver receives at most one value.
type OneshotReceiver[T any] struct{ s *oneshotState[T] }

// NewOneshot creates a oneshot channel.
func NewOneshot[T any]() (*OneshotSender[T], *OneshotReceiver[T]) {
	s := &oneshotState[T]{changed: make(chan struct{})}
	return &OneshotSender[T]{s}, &OneshotReceiver[T]{s}
}

// Send sends v. Any attempt uses the sender up: a second call returns
// ErrAlreadySent. It returns ErrClosed when the receiver is closed.
func (t *OneshotSender[T]) Send(v T) error {
	s := t.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.txUsed {
		return ErrAlreadySent
	}
	s.txUsed = true
	if s.rxClosed {
		return ErrClosed
	}
	s.value, s.hasValue = v, true
	s.broadcast()
	return nil
}

// Close drops the sender without sending. Idempotent.
func (t *OneshotSender[T]) Close() {
	s := t.s
	s.mu.Lock()
	s.txDropped = true
	s.broadcast()
	s.mu.Unlock()
}

// IsClosed reports whether the receiver is closed.
func (t *OneshotSender[T]) IsClosed() bool {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	return t.s.rxClosed
}

func (s *oneshotState[T]) tryRecvLocked() (T, error) {
	var zero T
	if s.hasValue && !s.taken {
		s.taken = true
		v := s.value
		s.value = zero
		return v, nil
	}
	if s.taken || s.rxClosed || s.txDropped {
		return zero, ErrClosed
	}
	return zero, ErrEmpty
}

// TryRecv takes the value without waiting; ErrEmpty or ErrClosed otherwise.
func (r *OneshotReceiver[T]) TryRecv() (T, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	return r.s.tryRecvLocked()
}

// Recv waits for the value; ErrClosed when none can arrive.
func (r *OneshotReceiver[T]) Recv() (T, error) { return r.RecvContext(context.Background()) }

// RecvContext is Recv that gives up with ctx.Err() when ctx is done.
func (r *OneshotReceiver[T]) RecvContext(ctx context.Context) (T, error) {
	s := r.s
	for {
		s.mu.Lock()
		v, err := s.tryRecvLocked()
		wait := s.changed
		s.mu.Unlock()
		if err != ErrEmpty {
			return v, err
		}
		select {
		case <-wait:
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err()
		}
	}
}

// Close stops the sender from sending; a value sent earlier can still be taken.
func (r *OneshotReceiver[T]) Close() {
	r.s.mu.Lock()
	r.s.rxClosed = true
	r.s.broadcast()
	r.s.mu.Unlock()
}
