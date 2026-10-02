package lombokasync

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestSpawnAwait(t *testing.T) {
	h := Spawn(func() int { return 42 })
	if h.Await() != 42 || h.Await() != 42 {
		t.Fatal("Await")
	}
	<-h.Done()
	p := Spawn(func() int { panic("boom") })
	defer func() {
		if r := recover(); r != "boom" {
			t.Fatalf("recovered %v", r)
		}
	}()
	p.Await()
}

func TestSleepAndTimeout(t *testing.T) {
	start := time.Now()
	Sleep(10 * time.Millisecond)
	if time.Since(start) < 10*time.Millisecond {
		t.Fatal("Sleep too short")
	}
	if v, err := Timeout(time.Second, func() int { return 1 }); v != 1 || err != nil {
		t.Fatal(v, err)
	}
	release := make(chan struct{})
	defer close(release)
	if _, err := Timeout(5*time.Millisecond, func() int { <-release; return 0 }); !errors.Is(err, ErrTimeout) {
		t.Fatal(err)
	}
	if ErrTimeout.Error() != "TIMEOUT: deadline elapsed" {
		t.Fatal(ErrTimeout.Error())
	}
	v, err := TimeoutCtx(context.Background(), 5*time.Millisecond, func(ctx context.Context) int {
		<-ctx.Done()
		return 7
	})
	if !(err == nil && v == 7) && !errors.Is(err, ErrTimeout) {
		t.Fatal(v, err)
	}
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := TimeoutCtx(parent, time.Second, func(context.Context) int { <-release; return 0 }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	if _, err := TimeoutErr(time.Second, func() (int, error) { return 0, boom }); err != boom {
		t.Fatal(err)
	}
	if _, err := TimeoutErr(5*time.Millisecond, func() (int, error) { <-release; return 0, nil }); !errors.Is(err, ErrTimeout) {
		t.Fatal(err)
	}
}

func TestInterval(t *testing.T) {
	ticks, stop := Interval(2 * time.Millisecond)
	for want := 0; want < 3; want++ {
		if n := <-ticks; n != want {
			t.Fatalf("tick %d, want %d", n, want)
		}
	}
	stop()
	stop()
	for range ticks {
	}
	ticks2, stop2 := Interval(time.Hour)
	stop2()
	if _, ok := <-ticks2; ok {
		t.Fatal("expected closed")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	Interval(0)
}

func TestMpscProducersAndClose(t *testing.T) {
	tx, rx := NewMpsc[int]()
	var wg sync.WaitGroup
	for p := 0; p < 4; p++ {
		s, err := tx.Clone()
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func(p int, s *MpscSender[int]) {
			defer wg.Done()
			defer s.Close()
			for i := 0; i < 500; i++ {
				if err := s.Send(p*1000 + i); err != nil {
					t.Error(err)
				}
			}
		}(p, s)
	}
	tx.Close()
	var got []int
	for {
		v, ok := rx.Recv()
		if !ok {
			break
		}
		got = append(got, v)
	}
	wg.Wait()
	sort.Ints(got)
	for i, v := range got {
		if want := (i/500)*1000 + i%500; v != want {
			t.Fatalf("got[%d] = %d, want %d", i, v, want)
		}
	}
	if len(got) != 2000 {
		t.Fatal(len(got))
	}
}

func TestMpscBoundedBlocking(t *testing.T) {
	tx, rx, err := NewBoundedMpsc[int](1)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for i := 0; i < 5; i++ {
			if err := tx.Send(i); err != nil {
				t.Error(err)
			}
		}
		tx.Close()
	}()
	for want := 0; ; want++ {
		v, ok := rx.Recv()
		if !ok {
			if want != 5 {
				t.Fatal(want)
			}
			break
		}
		if v != want {
			t.Fatal(v, want)
		}
	}
	if _, _, err := NewBoundedMpsc[int](0); !errors.Is(err, ErrInvalidCapacity) {
		t.Fatal(err)
	}
}

func TestMpscContextAndClose(t *testing.T) {
	tx, rx, _ := NewBoundedMpsc[int](1)
	_ = tx.TrySend(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := tx.SendContext(ctx, 2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	done := make(chan error)
	go func() { done <- tx.Send(3) }()
	time.Sleep(2 * time.Millisecond)
	rx.Close()
	if err := <-done; !errors.Is(err, ErrClosed) || !tx.IsClosed() {
		t.Fatal(err)
	}
	if rx.Len() != 1 {
		t.Fatal(rx.Len())
	}
	tx2, rx2 := NewMpsc[int]()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel2()
	if _, err := rx2.RecvContext(ctx2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	tx2.Close()
	tx2.Close()
	if err := tx2.TrySend(1); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := tx2.Send(1); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := tx2.Clone(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestMpscLongQueue(t *testing.T) {
	tx, rx := NewMpsc[int]()
	for i := 0; i < 5000; i++ {
		_ = tx.TrySend(i)
	}
	for i := 0; i < 5000; i++ {
		v, err := rx.TryRecv()
		if err != nil || v != i {
			t.Fatal(i, v, err)
		}
		if i%1000 == 0 {
			_ = tx.TrySend(-1)
		}
	}
	if rx.Len() != 5 {
		t.Fatal(rx.Len())
	}
}

func TestOneshot(t *testing.T) {
	tx, rx := NewOneshot[string]()
	go func() { time.Sleep(2 * time.Millisecond); _ = tx.Send("hi") }()
	if v, err := rx.Recv(); v != "hi" || err != nil {
		t.Fatal(v, err)
	}
	if _, err := rx.Recv(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	tx2, rx2 := NewOneshot[string]()
	go func() { time.Sleep(2 * time.Millisecond); tx2.Close() }()
	if _, err := rx2.Recv(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	tx3, rx3 := NewOneshot[int]()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
	defer cancel()
	if _, err := rx3.RecvContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	rx3.Close()
	if !tx3.IsClosed() || !errors.Is(tx3.Send(1), ErrClosed) || !errors.Is(tx3.Send(2), ErrAlreadySent) {
		t.Fatal("closed receiver")
	}
}

func TestCombinators(t *testing.T) {
	a, b := Join(func() int { return 1 }, func() string { return "x" })
	if a != 1 || b != "x" {
		t.Fatal(a, b)
	}
	x, y, z := Join3(func() int { return 1 }, func() int { return 2 }, func() int { return 3 })
	if x+y+z != 6 {
		t.Fatal(x, y, z)
	}
	got := JoinAll([]func() int{func() int { time.Sleep(3 * time.Millisecond); return 1 }, func() int { return 2 }})
	if got[0] != 1 || got[1] != 2 {
		t.Fatal(got)
	}
	slow := make(chan int)
	fast := make(chan int, 1)
	go func() { time.Sleep(2 * time.Millisecond); fast <- 9 }()
	if i, v, ok := Select(slow, fast); i != 1 || v != 9 || !ok {
		t.Fatal(i, v, ok)
	}
	closed := make(chan int)
	close(closed)
	if i, _, ok := Select(nil, closed); i != 1 || ok {
		t.Fatal(i, ok)
	}
	errs := make(chan error)
	go func() { time.Sleep(time.Millisecond); close(errs) }()
	if _, v, ok := Select[error](errs); v != nil || ok {
		t.Fatal(v, ok)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	Select[int]()
}
