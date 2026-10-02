<?php

declare(strict_types=1);

namespace LombokAsync\Executor;

/**
 * Cooperative event loop on PHP Fibers.
 *
 * Tasks run until they yield, sleep, or wait (on a task or a channel). The
 * loop keeps a ready queue and a timer heap; when nothing is ready it sleeps
 * until the earliest timer instead of polling.
 */
final class EventLoop
{
    /** @var \SplQueue<Task> */
    private \SplQueue $ready;
    /** @var \SplMinHeap<array{float, int, Task}> */
    private \SplMinHeap $timers;
    private int $timerSeq = 0;
    private int $finishedSeq = 0;
    private ?Task $running = null;

    private static ?EventLoop $current = null;

    public function __construct()
    {
        $this->ready = new \SplQueue();
        $this->timers = new \SplMinHeap();
    }

    /**
     * Runs $main as a task on a new loop until it finishes and returns its
     * result (or throws its error). Tasks still pending at that point are
     * abandoned.
     */
    public static function run(callable $main): mixed
    {
        $loop = new self();
        $previous = self::$current;
        self::$current = $loop;
        try {
            $task = $loop->spawn($main);
            $loop->runUntil($task);
            return $task->await();
        } finally {
            self::$current = $previous;
        }
    }

    /** The loop that is running now; throws \LogicException outside {@see run()}. */
    public static function current(): self
    {
        return self::$current ?? throw new \LogicException('no LombokAsync event loop is running; use EventLoop::run()');
    }

    /** Creates a task that starts at the loop's next scheduling step. */
    public function spawn(callable $fn): Task
    {
        $task = new Task($this, $fn);
        $this->wake($task);
        return $task;
    }

    /** Lets other ready tasks run before continuing. */
    public static function yield(): void
    {
        $loop = self::current();
        $loop->wake($loop->currentTask());
        $loop->park();
    }

    /** Suspends the current task for $ms milliseconds. */
    public function sleep(float $ms): void
    {
        $deadline = microtime(true) + max(0.0, $ms) / 1000.0;
        $task = $this->currentTask();
        while (microtime(true) < $deadline) {
            $this->wakeAt($deadline, $task);
            $this->park();
        }
    }

    /** @internal Schedules $task to be woken at $deadline (microtime seconds). */
    public function wakeAt(float $deadline, Task $task): void
    {
        $this->timers->insert([$deadline, $this->timerSeq++, $task]);
    }

    /**
     * @internal Suspends the current task until something calls wake() on it.
     * Callers re-check their condition after waking: wake-ups can be spurious.
     */
    public function park(): void
    {
        \Fiber::suspend();
    }

    /** @internal Puts $task in the ready queue (no-op when queued or done). */
    public function wake(Task $task): void
    {
        if (!$task->queued && !$task->isDone()) {
            $task->queued = true;
            $this->ready->enqueue($task);
        }
    }

    /** @internal The task that is running now. */
    public function currentTask(): Task
    {
        return $this->running ?? throw new \LogicException('this operation must run inside a LombokAsync task');
    }

    /** @internal */
    public function nextFinishedSeq(): int
    {
        return ++$this->finishedSeq;
    }

    private function runUntil(Task $main): void
    {
        while (!$main->isDone()) {
            $this->fireTimers();
            if ($this->ready->isEmpty()) {
                if ($this->timers->isEmpty()) {
                    throw new \LogicException('deadlock: every task is waiting and no timer is pending');
                }
                $wait = $this->timers->top()[0] - microtime(true);
                if ($wait > 0) {
                    usleep((int) ceil($wait * 1_000_000));
                }
                continue;
            }
            $task = $this->ready->dequeue();
            $task->queued = false;
            if ($task->isDone()) {
                continue;
            }
            $this->running = $task;
            try {
                $task->fiber->isStarted() ? $task->fiber->resume() : $task->fiber->start();
            } finally {
                $this->running = null;
            }
        }
    }

    private function fireTimers(): void
    {
        $now = microtime(true);
        while (!$this->timers->isEmpty() && $this->timers->top()[0] <= $now) {
            $this->wake($this->timers->extract()[2]);
        }
    }
}
