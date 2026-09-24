<?php

declare(strict_types=1);

namespace LombokAsync\Executor;

/**
 * Simple event loop using PHP Fibers for cooperative multitasking.
 *
 * Tasks are scheduled as Fibers and run cooperatively — each task
 * must yield control back to the loop by calling EventLoop::yield().
 */
class EventLoop
{
    /** @var \Fiber[] */
    private array $fibers = [];

    /** @var array<int, mixed> Task results indexed by task ID */
    private array $results = [];

    /** @var array<int, bool> Completed task IDs */
    private array $completed = [];

    private int $nextId = 0;

    private static ?EventLoop $current = null;

    /** Get the current running event loop. */
    public static function current(): ?EventLoop
    {
        return self::$current;
    }

    /**
     * Spawn a task (callable) on the event loop. Returns a task ID.
     */
    public function spawn(callable $fn): int
    {
        $id = $this->nextId++;
        $this->fibers[$id] = new \Fiber(function () use ($fn, $id) {
            $result = $fn();
            $this->results[$id] = $result;
            $this->completed[$id] = true;
            return $result;
        });
        return $id;
    }

    /**
     * Run the event loop until all tasks complete.
     *
     * @return mixed Result of the first spawned task, or null.
     */
    public function run(): mixed
    {
        $previous = self::$current;
        self::$current = $this;

        try {
            while (!empty($this->fibers)) {
                $remaining = [];
                foreach ($this->fibers as $id => $fiber) {
                    if ($fiber->isStarted()) {
                        if ($fiber->isSuspended()) {
                            $fiber->resume();
                        }
                    } else {
                        $fiber->start();
                    }

                    if (!$fiber->isTerminated()) {
                        $remaining[$id] = $fiber;
                    }
                }
                $this->fibers = $remaining;

                if (!empty($this->fibers)) {
                    usleep(1000); // 1ms yield
                }
            }
        } finally {
            self::$current = $previous;
        }

        return $this->results[0] ?? null;
    }

    /**
     * Check if a task has completed.
     */
    public function isCompleted(int $taskId): bool
    {
        return isset($this->completed[$taskId]);
    }

    /**
     * Get the result of a completed task.
     */
    public function getResult(int $taskId): mixed
    {
        return $this->results[$taskId] ?? null;
    }

    /**
     * Yield control back to the event loop from within a Fiber.
     */
    public static function yield(): void
    {
        if (\Fiber::getCurrent() !== null) {
            \Fiber::suspend();
        }
    }

    /**
     * Await a task by ID — yield until it completes.
     */
    public function await(int $taskId): mixed
    {
        while (!$this->isCompleted($taskId)) {
            self::yield();
        }
        return $this->results[$taskId] ?? null;
    }
}
