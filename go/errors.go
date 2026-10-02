// Package lombokasync provides async helpers for Go: task handles, timeouts,
// intervals, bounded and unbounded mpsc channels with explicit close rules,
// oneshot channels, and join/select combinators.
//
// Behaviour shared with the other LombokAsync ports (Rust, TypeScript,
// Python, PHP) is specified in docs/SPEC_LombokAsync_v0.2.0.md and checked
// against the shared vectors.
package lombokasync

// Error is the error type of this package. Code is stable across ports.
type Error struct {
	Code string
	Msg  string
}

func (e *Error) Error() string { return e.Code + ": " + e.Msg }

// Sentinel errors; compare with errors.Is.
var (
	ErrTimeout         = &Error{"TIMEOUT", "deadline elapsed"}
	ErrClosed          = &Error{"CLOSED", "channel is closed"}
	ErrFull            = &Error{"FULL", "channel is full"}
	ErrEmpty           = &Error{"EMPTY", "channel is empty"}
	ErrAlreadySent     = &Error{"ALREADY_SENT", "oneshot sender was already used"}
	ErrInvalidCapacity = &Error{"INVALID_CAPACITY", "capacity must be at least 1"}
)
