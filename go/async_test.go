package lombokasync

import (
	"testing"
	"time"
)

func TestSpawnAndAwait(t *testing.T) {
	h := Spawn(func() int { return 42 })
	if h.Await() != 42 {
		t.Error("expected 42")
	}
}

func TestMultipleSpawns(t *testing.T) {
	h1 := Spawn(func() int { return 1 })
	h2 := Spawn(func() int { return 2 })
	h3 := Spawn(func() int { return 3 })
	sum := h1.Await() + h2.Await() + h3.Await()
	if sum != 6 {
		t.Errorf("expected 6, got %d", sum)
	}
}

func TestSleep(t *testing.T) {
	start := time.Now()
	Sleep(50 * time.Millisecond)
	if time.Since(start) < 40*time.Millisecond {
		t.Error("sleep too short")
	}
}

func TestTimeoutOk(t *testing.T) {
	val, ok := Timeout(100*time.Millisecond, func() int { return 42 })
	if !ok || val != 42 {
		t.Errorf("expected (42, true), got (%d, %v)", val, ok)
	}
}

func TestTimeoutExpired(t *testing.T) {
	_, ok := Timeout(10*time.Millisecond, func() int {
		time.Sleep(200 * time.Millisecond)
		return 99
	})
	if ok {
		t.Error("expected timeout")
	}
}

func TestMpscChannel(t *testing.T) {
	tx, rx := MpscChannel[int](10)
	tx.Send(1)
	tx.Send(2)
	v1, ok1 := rx.Recv()
	v2, ok2 := rx.Recv()
	if !ok1 || v1 != 1 {
		t.Errorf("expected 1, got %d", v1)
	}
	if !ok2 || v2 != 2 {
		t.Errorf("expected 2, got %d", v2)
	}
}

func TestMpscClose(t *testing.T) {
	tx, rx := MpscChannel[int](10)
	tx.Send(99)
	tx.Close()
	v1, ok1 := rx.Recv()
	_, ok2 := rx.Recv()
	if !ok1 || v1 != 99 {
		t.Error("expected 99")
	}
	if ok2 {
		t.Error("expected closed")
	}
}

func TestOneshotChannel(t *testing.T) {
	tx, rx := OneshotChannel[int]()
	go func() { tx.Send(42) }()
	val := rx.Recv()
	if val != 42 {
		t.Errorf("expected 42, got %d", val)
	}
}

func TestJoin(t *testing.T) {
	a, b := Join(func() int { return 1 }, func() string { return "hello" })
	if a != 1 || b != "hello" {
		t.Error("join failed")
	}
}

func TestJoin3(t *testing.T) {
	a, b, c := Join3(
		func() int { return 1 },
		func() int { return 2 },
		func() int { return 3 },
	)
	if a+b+c != 6 {
		t.Error("join3 failed")
	}
}

func TestJoinAll(t *testing.T) {
	fns := []func() int{
		func() int { return 1 },
		func() int { return 2 },
		func() int { return 3 },
	}
	results := JoinAll(fns)
	if len(results) != 3 || results[0]+results[1]+results[2] != 6 {
		t.Error("joinAll failed")
	}
}

func TestSelect(t *testing.T) {
	result := Select(
		func() int { return 42 },
		func() int {
			time.Sleep(100 * time.Millisecond)
			return 99
		},
	)
	if !result.IsLeft || result.Left != 42 {
		t.Error("expected left 42")
	}
}

func TestInterval(t *testing.T) {
	ch, stop := Interval(20 * time.Millisecond)
	count := 0
	for tick := range ch {
		count++
		_ = tick
		if count >= 3 {
			close(stop)
			break
		}
	}
	if count != 3 {
		t.Errorf("expected 3 ticks, got %d", count)
	}
}
