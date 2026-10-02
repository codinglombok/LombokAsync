<?php

declare(strict_types=1);

namespace LombokAsync;

use LombokAsync\Executor\EventLoop;
use LombokAsync\Executor\Task;
use LombokAsync\Timer\Timer;

/**
 * Entry points: run a main task, spawn tasks, and combine them (SPEC section 5).
 * Everything except {@see run()} must be called from inside a task.
 */
final class Async
{
    /** Runs $main on a new event loop and returns its result. */
    public static function run(callable $main): mixed
    {
        return EventLoop::run($main);
    }

    /** Starts $fn as a task on the running loop. */
    public static function spawn(callable $fn): Task
    {
        return EventLoop::current()->spawn($fn);
    }

    /** Lets other ready tasks run before continuing. */
    public static function yield(): void
    {
        EventLoop::yield();
    }

    /** Suspends the current task for $ms milliseconds. */
    public static function sleep(float $ms): void
    {
        Timer::sleep($ms);
    }

    /** See {@see Timer::timeout()}. */
    public static function timeout(float $ms, callable $fn): mixed
    {
        return Timer::timeout($ms, $fn);
    }

    /**
     * Runs every callable as a task and waits for all of them. Returns the
     * results in input order, or throws the error of the lowest-index task
     * that failed (not the earliest one).
     *
     * @param list<callable> $fns
     * @return list<mixed>
     */
    public static function joinAll(array $fns): array
    {
        $loop = EventLoop::current();
        $tasks = array_map(static fn (callable $fn): Task => $loop->spawn($fn), array_values($fns));
        $results = [];
        $firstError = null;
        foreach ($tasks as $t) {
            try {
                $results[] = $t->await();
            } catch (\Throwable $e) {
                $firstError ??= $e;
                $results[] = null;
            }
        }
        if ($firstError !== null) {
            throw $firstError;
        }
        return $results;
    }

    /**
     * Runs every callable as a task and returns [index, value] of the first
     * to finish, or throws its error. Ties go to the lowest index. The other
     * tasks are cancelled.
     *
     * @param list<callable> $fns
     * @return array{0: int, 1: mixed}
     */
    public static function select(array $fns): array
    {
        if ($fns === []) {
            throw new \InvalidArgumentException('select needs at least one callable');
        }
        $loop = EventLoop::current();
        $tasks = array_map(static fn (callable $fn): Task => $loop->spawn($fn), array_values($fns));
        $self = $loop->currentTask();
        while (true) {
            $winner = null;
            foreach ($tasks as $i => $t) {
                if ($t->isDone() && ($winner === null || $t->finishedSeq < $tasks[$winner]->finishedSeq)) {
                    $winner = $i;
                }
            }
            if ($winner !== null) {
                break;
            }
            foreach ($tasks as $t) {
                $t->waiters[] = $self;
            }
            $loop->park();
        }
        foreach ($tasks as $i => $t) {
            if ($i !== $winner) {
                $t->cancel();
            }
        }
        return [$winner, $tasks[$winner]->await()];
    }
}
