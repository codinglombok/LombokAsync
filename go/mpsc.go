package lombokasync

import (
	"context"
	"sync"
)

// mpscState is shared by every sender and the receiver. changed is closed and
// replaced on every state change so blocked callers can wait with select.
type mpscState[T any] struct {
	mu       sync.Mutex
	queue    []T
	head     int
	capacity int // 0 = unbounded
	senders  int
	rxClosed bool
	changed  chan struct{}
}

func (s *mpscState[T]) broadcast() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *mpscState[T]) length() int { return len(s.queue) - s.head }

// MpscSender is a sending half of an mpsc channel. Use Clone for more senders.
type MpscSender[T any] struct {
	s       *mpscState[T]
	mu      sync.Mutex
	dropped bool
}

// MpscReceiver is the receiving half of an mpsc channel.
type MpscReceiver[T any] struct {
	s *mpscState[T]
}

func newMpsc[T any](capacity int) (*MpscSender[T], *MpscReceiver[T]) {
	s := &mpscState[T]{capacity: capacity, senders: 1, changed: make(chan struct{})}
	return &MpscSender[T]{s: s}, &MpscReceiver[T]{s: s}
}

// NewMpsc creates an unbounded mpsc channel.
func NewMpsc[T any]() (*MpscSender[T], *MpscReceiver[T]) { return newMpsc[T](0) }

// NewBoundedMpsc creates a channel that holds at most capacity values. It
// returns ErrInvalidCapacity when capacity < 1.
func NewBoundedMpsc[T any](capacity int) (*MpscSender[T], *MpscReceiver[T], error) {
	if capacity < 1 {
		return nil, nil, ErrInvalidCapacity
	}
	tx, rx := newMpsc[T](capacity)
	return tx, rx, nil
}

func (t *MpscSender[T]) isDropped() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.dropped
}

// TrySend queues v without waiting. It returns ErrClosed when the receiver
// is closed (or this sender was closed) and ErrFull when a bounded channel is full.
func (t *MpscSender[T]) TrySend(v T) error {
	if t.isDropped() {
		return ErrClosed
	}
	s := t.s
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.trySendLocked(v)
}

func (s *mpscState[T]) trySendLocked(v T) error {
	if s.rxClosed {
		return ErrClosed
	}
	if s.capacity > 0 && s.length() >= s.capacity {
		return ErrFull
	}
	s.queue = append(s.queue, v)
	s.broadcast()
	return nil
}

// Send queues v, waiting for space when the channel is bounded and full.
func (t *MpscSender[T]) Send(v T) error { return t.SendContext(context.Background(), v) }

// SendContext is Send that gives up with ctx.Err() when ctx is done.
func (t *MpscSender[T]) SendContext(ctx context.Context, v T) error {
	if t.isDropped() {
		return ErrClosed
	}
	s := t.s
	for {
		s.mu.Lock()
		err := s.trySendLocked(v)
		wait := s.changed
		s.mu.Unlock()
		if err != ErrFull {
			return err
		}
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Clone returns another sender for the same channel, or ErrClosed when this
// sender was closed.
func (t *MpscSender[T]) Clone() (*MpscSender[T], error) {
	if t.isDropped() {
		return nil, ErrClosed
	}
	t.s.mu.Lock()
	t.s.senders++
	t.s.mu.Unlock()
	return &MpscSender[T]{s: t.s}, nil
}

// Close drops this sender. When the last sender is closed the channel closes
// for receiving once the queue is empty. Close is idempotent.
func (t *MpscSender[T]) Close() {
	t.mu.Lock()
	if t.dropped {
		t.mu.Unlock()
		return
	}
	t.dropped = true
	t.mu.Unlock()
	s := t.s
	s.mu.Lock()
	s.senders--
	s.broadcast()
	s.mu.Unlock()
}

// IsClosed reports whether the receiver is closed.
func (t *MpscSender[T]) IsClosed() bool {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	return t.s.rxClosed
}

func (s *mpscState[T]) tryRecvLocked() (T, error) {
	var zero T
	if s.length() > 0 {
		v := s.queue[s.head]
		s.queue[s.head] = zero
		s.head++
		if s.head == len(s.queue) {
			s.queue, s.head = s.queue[:0], 0
		} else if s.head > 1024 && s.head*2 > len(s.queue) {
			s.queue = append([]T(nil), s.queue[s.head:]...)
			s.head = 0
		}
		s.broadcast()
		return v, nil
	}
	if s.rxClosed || s.senders == 0 {
		return zero, ErrClosed
	}
	return zero, ErrEmpty
}

// TryRecv takes the next value without waiting. It returns ErrEmpty when
// nothing is queued but a sender is alive, and ErrClosed when nothing more
// can arrive.
func (r *MpscReceiver[T]) TryRecv() (T, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	return r.s.tryRecvLocked()
}

// Recv waits for the next value. ok is false once the channel is closed and empty.
func (r *MpscReceiver[T]) Recv() (v T, ok bool) {
	v, err := r.RecvContext(context.Background())
	return v, err == nil
}

// RecvContext waits for the next value; it returns ErrClosed once the channel
// is closed and empty, or ctx.Err() when ctx is done.
func (r *MpscReceiver[T]) RecvContext(ctx context.Context) (T, error) {
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

// Close stops new sends; values already queued can still be received.
func (r *MpscReceiver[T]) Close() {
	r.s.mu.Lock()
	r.s.rxClosed = true
	r.s.broadcast()
	r.s.mu.Unlock()
}

// Len returns the number of queued values.
func (r *MpscReceiver[T]) Len() int {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	return r.s.length()
}
