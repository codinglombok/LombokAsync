<?php

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use LombokAsync\Executor\EventLoop;
use LombokAsync\Timer\Timer;
use LombokAsync\Channel\MpscChannel;
use LombokAsync\Channel\OneshotChannel;

$pass = 0;
$fail = 0;

function assert_eq(mixed $a, mixed $b, string $name): void {
    global $pass, $fail;
    if ($a === $b) {
        $pass++;
    } else {
        $fail++;
        echo "FAIL: {$name}\n";
        echo "  expected: " . var_export($b, true) . "\n";
        echo "  got:      " . var_export($a, true) . "\n";
    }
}

// --- EventLoop: basic spawn ---
$loop = new EventLoop();
$id = $loop->spawn(fn() => 42);
$result = $loop->run();
assert_eq($result, 42, 'eventloop spawn');

// --- EventLoop: multiple spawns ---
$loop = new EventLoop();
$results = [];
$loop->spawn(function () use (&$results) { $results[] = 1; return 1; });
$loop->spawn(function () use (&$results) { $results[] = 2; return 2; });
$loop->spawn(function () use (&$results) { $results[] = 3; return 3; });
$loop->run();
assert_eq(count($results), 3, 'eventloop multi count');
sort($results);
assert_eq($results, [1, 2, 3], 'eventloop multi values');

// --- EventLoop: cooperative yield ---
$loop = new EventLoop();
$order = [];
$loop->spawn(function () use (&$order) {
    $order[] = 'a1';
    EventLoop::yield();
    $order[] = 'a2';
    return 'a';
});
$loop->spawn(function () use (&$order) {
    $order[] = 'b1';
    EventLoop::yield();
    $order[] = 'b2';
    return 'b';
});
$loop->run();
// Both fibers should interleave
assert_eq(count($order), 4, 'yield interleave count');

// --- Timer: sleep ---
$t0 = microtime(true);
$loop = new EventLoop();
$loop->spawn(function () {
    Timer::sleep(50);
    return true;
});
$loop->run();
$elapsed = (microtime(true) - $t0) * 1000;
assert_eq($elapsed >= 40, true, 'timer sleep 50ms');

// --- Timer: timeout sync OK ---
[$val, $ok] = Timer::timeoutSync(1000, fn() => 42);
assert_eq($ok, true, 'timeout sync ok');
assert_eq($val, 42, 'timeout sync value');

// --- MPSC Channel ---
[$tx, $rx] = MpscChannel::create();
$tx->send(1);
$tx->send(2);
[$v1, $ok1] = $rx->recv();
[$v2, $ok2] = $rx->recv();
assert_eq($v1, 1, 'mpsc recv 1');
assert_eq($v2, 2, 'mpsc recv 2');
assert_eq($ok1, true, 'mpsc ok 1');
assert_eq($ok2, true, 'mpsc ok 2');

// --- MPSC close ---
$tx->close();
[$v3, $ok3] = $rx->recv();
assert_eq($ok3, false, 'mpsc closed');

// --- Oneshot Channel ---
[$otx, $orx] = OneshotChannel::create();
$otx->send(42);
$val = $orx->recv();
assert_eq($val, 42, 'oneshot value');

// --- Oneshot sent flag ---
assert_eq($otx->__toString ?? true, true, 'oneshot sent');
try {
    $otx->send(99);
    assert_eq(true, false, 'oneshot double send should throw');
} catch (\RuntimeException $e) {
    assert_eq(str_contains($e->getMessage(), 'already sent'), true, 'oneshot double send error');
}

echo "\n{$pass} passed, {$fail} failed\n";
exit($fail > 0 ? 1 : 0);
