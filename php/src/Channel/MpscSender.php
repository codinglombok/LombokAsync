<?php

declare(strict_types=1);

namespace LombokAsync\Channel;

use LombokAsync\AsyncException;
use LombokAsync\Executor\EventLoop;

/**
 * Sending half of an mpsc channel. Use {@see clone()} for more senders.
 */
final class MpscSender
{
    private bool $dropped = false;

    /** @internal */
    public function __construct(private readonly MpscChannel $c)
    {
        $c->senders++;
    }

    /**
     * Queues $value without waiting.
     *
     * @throws AsyncException CLOSED when the receiver (or this sender) is closed, FULL when a bounded channel is full
     */
    public function trySend(mixed $value): void
    {
        $this->alive();
        $c = $this->c;
        if ($c->rxClosed) {
            throw new AsyncException(AsyncException::CLOSED, 'channel is closed');
        }
        if ($c->capacity !== null && $c->queue->count() >= $c->capacity) {
            throw new AsyncException(AsyncException::FULL, 'channel is full');
        }
        $c->queue->enqueue($value);
        MpscChannel::wakeAll($c->recvWaiters);
    }

    /**
     * Queues $value, waiting (inside a task) for space when the channel is full.
     *
     * @throws AsyncException CLOSED when the receiver closes
     */
    public function send(mixed $value): void
    {
        while (true) {
            try {
                $this->trySend($value);
                return;
            } catch (AsyncException $e) {
                if ($e->errorCode !== AsyncException::FULL) {
                    throw $e;
                }
            }
            $loop = EventLoop::current();
            $this->c->sendWaiters[] = $loop->currentTask();
            $loop->park();
        }
    }

    /** Returns another sender for the same channel. */
    public function clone(): self
    {
        $this->alive();
        return new self($this->c);
    }

    /** Drops this sender; the channel closes after the last one. Idempotent. */
    public function close(): void
    {
        if ($this->dropped) {
            return;
        }
        $this->dropped = true;
        $this->c->senders--;
        if ($this->c->senders === 0) {
            MpscChannel::wakeAll($this->c->recvWaiters);
        }
    }

    /** True when the receiver is closed. */
    public function isClosed(): bool
    {
        return $this->c->rxClosed;
    }

    private function alive(): void
    {
        if ($this->dropped) {
            throw new AsyncException(AsyncException::CLOSED, 'sender was closed');
        }
    }
}
