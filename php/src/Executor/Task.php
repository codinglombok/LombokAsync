<?php

declare(strict_types=1);

namespace LombokAsync\Executor;

use LombokAsync\AsyncException;

/**
 * A task running on an {@see EventLoop}. Await it from another task.
 */
final class Task
{
    /** @internal */
    public \Fiber $fiber;
    /** @internal true while the task sits in the ready queue */
    public bool $queued = false;
    /** @internal completion order within the loop (0 until done) */
    public int $finishedSeq = 0;
    /** @var list<Task> @internal tasks to wake when this one finishes */
    public array $waiters = [];

    private bool $done = false;
    private mixed $result = null;
    private ?\Throwable $error = null;

    /** @internal */
    public function __construct(private readonly EventLoop $loop, callable $fn)
    {
        $this->fiber = new \Fiber(function () use ($fn): void {
            try {
                $this->result = $fn();
            } catch (\Throwable $e) {
                $this->error = $e;
            }
            $this->finish();
        });
    }

    /** @internal */
    public function finish(): void
    {
        $this->done = true;
        $this->finishedSeq = $this->loop->nextFinishedSeq();
        $waiters = $this->waiters;
        $this->waiters = [];
        foreach ($waiters as $w) {
            $this->loop->wake($w);
        }
    }

    public function isDone(): bool
    {
        return $this->done;
    }

    /**
     * Waits for the task and returns its result, or throws its error.
     * Must be called from inside a task of the same loop.
     */
    public function await(): mixed
    {
        while (!$this->done) {
            $this->waiters[] = $this->loop->currentTask();
            $this->loop->park();
        }
        if ($this->error !== null) {
            throw $this->error;
        }
        return $this->result;
    }

    /**
     * Stops the task: it is never resumed again and awaiting it throws
     * AsyncException CANCELLED. Has no effect on a finished task.
     */
    public function cancel(): void
    {
        if ($this->done) {
            return;
        }
        $this->error = new AsyncException(AsyncException::CANCELLED, 'task was cancelled');
        $this->finish();
    }
}
