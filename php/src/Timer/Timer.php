<?php

declare(strict_types=1);

namespace LombokAsync\Timer;

use LombokAsync\Executor\EventLoop;

/**
 * Timer utilities for cooperative async.
 */
class Timer
{
    /**
     * Sleep for the given number of milliseconds (cooperative — yields to event loop).
     */
    public static function sleep(int $ms): void
    {
        $deadline = microtime(true) + ($ms / 1000.0);
        while (microtime(true) < $deadline) {
            EventLoop::yield();
        }
    }

    /**
     * Run a callable with a timeout. Returns [result, true] if it completes in time,
     * or [null, false] if the deadline elapses.
     *
     * Note: Only works with cooperative tasks that call EventLoop::yield().
     * For blocking operations, use the non-cooperative version.
     *
     * @return array{0: mixed, 1: bool}
     */
    public static function timeout(int $ms, callable $fn): array
    {
        $deadline = microtime(true) + ($ms / 1000.0);
        $loop = new EventLoop();
        $taskId = $loop->spawn($fn);

        while (!$loop->isCompleted($taskId)) {
            if (microtime(true) >= $deadline) {
                return [null, false];
            }
            // Run one tick manually
            try {
                $fiber = (new \ReflectionObject($loop))->getProperty('fibers');
                $fiber->setAccessible(true);
                $fibers = $fiber->getValue($loop);
                foreach ($fibers as $f) {
                    if (!$f->isStarted()) {
                        $f->start();
                    } elseif ($f->isSuspended()) {
                        $f->resume();
                    }
                }
            } catch (\Throwable) {
                break;
            }
            usleep(1000);
        }

        if ($loop->isCompleted($taskId)) {
            return [$loop->getResult($taskId), true];
        }
        return [null, false];
    }

    /**
     * Simple synchronous timeout — runs fn and checks wall-clock time.
     *
     * @return array{0: mixed, 1: bool}
     */
    public static function timeoutSync(int $ms, callable $fn): array
    {
        $start = microtime(true);
        $result = $fn();
        $elapsed = (microtime(true) - $start) * 1000;
        if ($elapsed > $ms) {
            return [null, false];
        }
        return [$result, true];
    }
}
