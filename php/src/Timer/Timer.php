<?php

declare(strict_types=1);

namespace LombokAsync\Timer;

use LombokAsync\AsyncException;
use LombokAsync\Executor\EventLoop;

/**
 * Timers for tasks on the running {@see EventLoop}.
 */
final class Timer
{
    /** Suspends the current task for $ms milliseconds; other tasks keep running. */
    public static function sleep(float $ms): void
    {
        EventLoop::current()->sleep($ms);
    }

    /**
     * Runs $fn as a task and returns its result, or throws AsyncException
     * TIMEOUT after $ms milliseconds (the task is then cancelled). Errors from
     * $fn pass through. A task that finishes in the same step as the deadline wins.
     */
    public static function timeout(float $ms, callable $fn): mixed
    {
        $loop = EventLoop::current();
        $deadline = microtime(true) + max(0.0, $ms) / 1000.0;
        $task = $loop->spawn($fn);
        $self = $loop->currentTask();
        while (!$task->isDone()) {
            if (microtime(true) >= $deadline) {
                $task->cancel();
                throw new AsyncException(AsyncException::TIMEOUT, "deadline of {$ms} ms elapsed");
            }
            $task->waiters[] = $self;
            $loop->wakeAt($deadline, $self);
            $loop->park();
        }
        return $task->await();
    }

    /**
     * Yields 0, 1, 2, ... every $ms milliseconds. Ticks are scheduled at fixed
     * multiples of the period, so delays do not accumulate.
     *
     * @return \Generator<int, int>
     */
    public static function interval(float $ms): \Generator
    {
        if (!($ms > 0)) {
            throw new \InvalidArgumentException('interval period must be greater than zero');
        }
        $start = microtime(true);
        for ($n = 0; ; $n++) {
            $wait = ($start + ($n + 1) * $ms / 1000.0 - microtime(true)) * 1000.0;
            self::sleep(max(0.0, $wait));
            yield $n;
        }
    }
}
