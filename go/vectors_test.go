package lombokasync

// Runs the shared cross-language vectors (vectors/lombokasync-vectors-v1.json).

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"runtime"
	"testing"
	"time"
)

type taskSpec struct {
	Yields int     `json:"yields"`
	Value  any     `json:"value"`
	Error  *string `json:"error"`
	Never  bool    `json:"never"`
}

type vectorCase struct {
	ID       string            `json:"id"`
	Kind     string            `json:"kind"`
	Ops      []json.RawMessage `json:"ops"`
	Tasks    []taskSpec        `json:"tasks"`
	Task     *taskSpec         `json:"task"`
	Expected json.RawMessage   `json:"expected"`
}

type vectorDoc struct {
	TimeoutMs int          `json:"timeout_deadline_ms"`
	Cases     []vectorCase `json:"cases"`
}

func decode(raw []byte, v any) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(v); err != nil {
		panic(err)
	}
}

func canonical(v any) string {
	var x any
	b, _ := json.Marshal(v)
	decode(b, &x)
	out, _ := json.Marshal(x)
	return string(out)
}

func status(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrFull):
		return "full"
	case errors.Is(err, ErrClosed):
		return "closed"
	case errors.Is(err, ErrEmpty):
		return "empty"
	case errors.Is(err, ErrAlreadySent):
		return "already_sent"
	}
	panic(err)
}

func runMpsc(ops []json.RawMessage) []any {
	var out []any
	var senders []*MpscSender[any]
	var rx *MpscReceiver[any]
	for _, raw := range ops {
		var op []any
		decode(raw, &op)
		idx := func(i int) int { n, _ := op[i].(json.Number).Int64(); return int(n) }
		switch op[0] {
		case "new":
			var tx *MpscSender[any]
			if op[1] == nil {
				tx, rx = NewMpsc[any]()
			} else {
				var err error
				tx, rx, err = NewBoundedMpsc[any](idx(1))
				if errors.Is(err, ErrInvalidCapacity) {
					return append(out, "invalid_capacity")
				}
			}
			senders = append(senders, tx)
			out = append(out, "ok")
		case "clone":
			s, err := senders[idx(1)].Clone()
			if err != nil {
				panic(err)
			}
			senders = append(senders, s)
			out = append(out, map[string]any{"sender": len(senders) - 1})
		case "send":
			out = append(out, status(senders[idx(1)].TrySend(op[2])))
		case "recv":
			v, err := rx.TryRecv()
			if err == nil {
				out = append(out, map[string]any{"value": v})
			} else {
				out = append(out, status(err))
			}
		case "drop":
			senders[idx(1)].Close()
			out = append(out, "ok")
		case "close":
			rx.Close()
			out = append(out, "ok")
		case "len":
			out = append(out, map[string]any{"len": rx.Len()})
		default:
			panic(op[0])
		}
	}
	return out
}

func runOneshot(ops []json.RawMessage) []any {
	tx, rx := NewOneshot[any]()
	var out []any
	for _, raw := range ops {
		var op []any
		decode(raw, &op)
		switch op[0] {
		case "new":
			out = append(out, "ok")
		case "send":
			out = append(out, status(tx.Send(op[1])))
		case "recv":
			v, err := rx.TryRecv()
			if err == nil {
				out = append(out, map[string]any{"value": v})
			} else {
				out = append(out, status(err))
			}
		case "drop_tx":
			tx.Close()
			out = append(out, "ok")
		case "close":
			rx.Close()
			out = append(out, "ok")
		default:
			panic(op[0])
		}
	}
	return out
}

func taskFn(t taskSpec, stop <-chan struct{}) func() (any, error) {
	return func() (any, error) {
		if t.Never {
			<-stop
			return nil, nil
		}
		for i := 0; i < t.Yields; i++ {
			runtime.Gosched()
		}
		if t.Error != nil {
			return nil, errors.New(*t.Error)
		}
		return t.Value, nil
	}
}

type result struct {
	v   any
	err error
}

func TestVectors(t *testing.T) {
	raw, err := os.ReadFile("../vectors/lombokasync-vectors-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc vectorDoc
	decode(raw, &doc)
	if len(doc.Cases) < 100 {
		t.Fatalf("only %d cases", len(doc.Cases))
	}
	stop := make(chan struct{})
	defer close(stop)
	for _, c := range doc.Cases {
		var got any
		switch c.Kind {
		case "mpsc":
			got = runMpsc(c.Ops)
		case "oneshot":
			got = runOneshot(c.Ops)
		case "join_all":
			fns := make([]func() (any, error), len(c.Tasks))
			for i, ts := range c.Tasks {
				fns[i] = taskFn(ts, stop)
			}
			vals, err := TryJoinAll(fns)
			if err != nil {
				got = map[string]any{"error": err.Error()}
			} else {
				got = map[string]any{"ok": vals}
			}
		case "select":
			// Go idiom: a ready task is a channel holding its result, a task that
			// never finishes is a nil channel.
			chans := make([]<-chan result, len(c.Tasks))
			for i, ts := range c.Tasks {
				if ts.Never {
					continue
				}
				ch := make(chan result, 1)
				v, err := taskFn(ts, stop)()
				ch <- result{v, err}
				chans[i] = ch
			}
			i, r, _ := Select(chans...)
			if r.err != nil {
				got = map[string]any{"error": r.err.Error()}
			} else {
				got = map[string]any{"index": i, "value": r.v}
			}
		case "timeout":
			v, err := TimeoutErr(time.Duration(doc.TimeoutMs)*time.Millisecond, taskFn(*c.Task, stop))
			switch {
			case errors.Is(err, ErrTimeout):
				got = "timeout"
			case err != nil:
				got = map[string]any{"error": err.Error()}
			default:
				got = map[string]any{"ok": v}
			}
		default:
			t.Fatalf("unknown kind %s", c.Kind)
		}
		var want any
		decode(c.Expected, &want)
		if canonical(got) != canonical(want) {
			t.Errorf("%s: got %s want %s", c.ID, canonical(got), canonical(want))
		}
	}
}
