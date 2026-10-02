<?php

declare(strict_types=1);

require_once __DIR__ . '/bootstrap.php';

use LombokAsync\Async;
use LombokAsync\AsyncException;
use LombokAsync\Channel\MpscChannel;
use LombokAsync\Channel\OneshotChannel;
use LombokAsync\Executor\EventLoop;
use LombokAsync\Timer\Timer;

// run / spawn / await / interleaving
$log = [];
$r = Async::run(static function () use (&$log): int {
    $a = Async::spawn(static function () use (&$log): int {
        for ($i = 0; $i < 3; $i++) {
            $log[] = "a$i";
            Async::yield();
        }
        return 1;
    });
    $b = Async::spawn(static function () use (&$log): int {
        for ($i = 0; $i < 3; $i++) {
            $log[] = "b$i";
            Async::yield();
        }
        return 2;
    });
    return $a->await() + $b->await();
});
check($r === 3, 'run returns main result');
check($log === ['a0', 'b0', 'a1', 'b1', 'a2', 'b2'], 'tasks interleave: ' . implode(',', $log));

// errors propagate through await and run
try {
    Async::run(static function (): void {
        Async::spawn(static fn () => throw new \DomainException('inner'))->await();
    });
    check(false, 'error not propagated');
} catch (\DomainException $e) {
    check($e->getMessage() === 'inner', 'error propagated');
}

// operations outside a loop
try {
    EventLoop::current();
    check(false, 'current() outside run');
} catch (\LogicException) {
    check(true, 'current() outside run');
}
try {
    Async::run(static function (): void {
        (new EventLoop())->currentTask();
    });
    check(false, 'currentTask outside task');
} catch (\LogicException) {
    check(true, 'currentTask outside task');
}

// deadlock detection
try {
    Async::run(static function (): void {
        [, $rx] = MpscChannel::unbounded();
        $rx->recv();
    });
    check(false, 'deadlock not detected');
} catch (\LogicException $e) {
    check(str_contains($e->getMessage(), 'deadlock'), 'deadlock detected');
}

// sleep and timers run in deadline order without blocking other tasks
$order = [];
$t0 = microtime(true);
Async::run(static function () use (&$order): void {
    $tasks = [];
    foreach ([30, 10, 20] as $ms) {
        $tasks[] = Async::spawn(static function () use ($ms, &$order): void {
            Async::sleep($ms);
            $order[] = $ms;
        });
    }
    Async::joinAll(array_map(static fn ($t) => static fn () => $t->await(), $tasks));
});
check($order === [10, 20, 30], 'sleeps finish in deadline order');
$elapsed = (microtime(true) - $t0) * 1000;
check($elapsed >= 29 && $elapsed < 200, "sleeps run concurrently ($elapsed ms)");

// timeout
Async::run(static function (): void {
    check(Async::timeout(100, static fn () => 5) === 5, 'timeout ok');
    throwsCode(static fn () => Async::timeout(10, static fn () => Async::sleep(1000)), AsyncException::TIMEOUT, 'timeout elapsed');
    try {
        Async::timeout(100, static fn () => throw new \InvalidArgumentException('x'));
        check(false, 'timeout error');
    } catch (\InvalidArgumentException) {
        check(true, 'timeout error passes through');
    }
});

// cancel
Async::run(static function (): void {
    $t = Async::spawn(static fn () => Async::sleep(1000));
    Async::yield();
    $t->cancel();
    $t->cancel();
    throwsCode(static fn () => $t->await(), AsyncException::CANCELLED, 'cancelled task');
    $done = Async::spawn(static fn () => 1);
    check($done->await() === 1, 'await finished');
    $done->cancel();
    check($done->await() === 1, 'cancel after finish has no effect');
});

// interval
Async::run(static function (): void {
    $ticks = [];
    foreach (Timer::interval(5) as $n) {
        $ticks[] = $n;
        if ($n === 2) {
            break;
        }
    }
    check($ticks === [0, 1, 2], 'interval ticks');
});
try {
    Timer::interval(0)->current();
    check(false, 'interval 0');
} catch (\InvalidArgumentException) {
    check(true, 'interval 0 rejected');
}

// mpsc: producers and foreach
$got = Async::run(static function (): array {
    [$tx, $rx] = MpscChannel::bounded(1);
    $tx2 = $tx->clone();
    foreach ([[$tx, [0, 1, 2]], [$tx2, [100]]] as [$s, $values]) {
        Async::spawn(static function () use ($s, $values): void {
            foreach ($values as $v) {
                $s->send($v);
            }
            $s->close();
        });
    }
    $got = [];
    foreach ($rx as $v) {
        $got[] = $v;
    }
    sort($got);
    return $got;
});
check($got === [0, 1, 2, 100], 'mpsc producers: ' . json_encode($got));

// mpsc: closing the receiver wakes a blocked sender and receiver
Async::run(static function (): void {
    [$tx, $rx] = MpscChannel::bounded(1);
    $tx->trySend(1);
    $pending = Async::spawn(static fn () => $tx->send(2));
    [, $rx2] = MpscChannel::unbounded();
    $waiting = Async::spawn(static fn () => $rx2->recv());
    Async::yield();
    $rx->close();
    $rx2->close();
    check($tx->isClosed(), 'isClosed');
    throwsCode(static fn () => $pending->await(), AsyncException::CLOSED, 'pending send after close');
    throwsCode(static fn () => $waiting->await(), AsyncException::CLOSED, 'pending recv after close');
    check(count($rx) === 1, 'queued value kept');
});

// mpsc: misc errors
[$tx] = MpscChannel::create();
$tx->close();
$tx->close();
throwsCode(static fn () => $tx->trySend(1), AsyncException::CLOSED, 'closed sender');
throwsCode(static fn () => $tx->clone(), AsyncException::CLOSED, 'clone closed sender');
throwsCode(static fn () => MpscChannel::bounded(0), AsyncException::INVALID_CAPACITY, 'capacity 0');
$e = new AsyncException(AsyncException::FULL, 'x');
check($e->getMessage() === 'FULL: x' && $e instanceof \RuntimeException, 'exception shape');

// oneshot
Async::run(static function (): void {
    [$tx, $rx] = OneshotChannel::create();
    Async::spawn(static function () use ($tx): void {
        Async::sleep(2);
        $tx->send('hi');
    });
    check($rx->recv() === 'hi', 'oneshot recv');
    throwsCode(static fn () => $rx->recv(), AsyncException::CLOSED, 'oneshot recv twice');
    [$tx2, $rx2] = OneshotChannel::create();
    Async::spawn(static fn () => $tx2->close());
    throwsCode(static fn () => $rx2->recv(), AsyncException::CLOSED, 'oneshot sender dropped');
    [$tx3, $rx3] = OneshotChannel::create();
    $rx3->close();
    check($tx3->isClosed(), 'oneshot isClosed');
});

// combinators
Async::run(static function (): void {
    check(Async::joinAll([]) === [], 'joinAll empty');
    try {
        Async::joinAll([
            static function (): void {
                Async::sleep(5);
                throw new \RuntimeException('late');
            },
            static fn () => throw new \RuntimeException('early'),
        ]);
        check(false, 'joinAll error');
    } catch (\RuntimeException $e) {
        check($e->getMessage() === 'late', 'joinAll lowest index error');
    }
    $r = Async::select([static fn () => Async::sleep(50), static function (): int {
        Async::sleep(1);
        return 2;
    }]);
    check($r === [1, 2], 'select first finished');
    try {
        Async::select([]);
        check(false, 'select empty');
    } catch (\InvalidArgumentException) {
        check(true, 'select empty rejected');
    }
});

finish('api');
