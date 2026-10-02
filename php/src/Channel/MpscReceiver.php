<?php

declare(strict_types=1);

namespace LombokAsync\Channel;

use LombokAsync\AsyncException;
use LombokAsync\Executor\EventLoop;

/**
 * Receiving half of an mpsc channel. Iterate it with foreach inside a task.
 *
 * @implements \IteratorAggregate<int, mixed>
 */
final class MpscReceiver implements \IteratorAggregate, \Countable
{
    /** @internal */
    public function __construct(private readonly MpscChannel $c)
    {
    }

    /**
     * Takes the next value without waiting.
     *
     * @throws AsyncException EMPTY when nothing is queued but a sender is alive, CLOSED when nothing more can arrive
     */
    public function tryRecv(): mixed
    {
        $c = $this->c;
        if (!$c->queue->isEmpty()) {
            $value = $c->queue->dequeue();
            MpscChannel::wakeAll($c->sendWaiters);
            return $value;
        }
        if ($c->rxClosed || $c->senders === 0) {
            throw new AsyncException(AsyncException::CLOSED, 'channel is closed');
        }
        throw new AsyncException(AsyncException::EMPTY, 'channel is empty');
    }

    /**
     * Waits (inside a task) for the next value.
     *
     * @throws AsyncException CLOSED once the channel is closed and empty
     */
    public function recv(): mixed
    {
        while (true) {
            try {
                return $this->tryRecv();
            } catch (AsyncException $e) {
                if ($e->errorCode !== AsyncException::EMPTY) {
                    throw $e;
                }
            }
            $loop = EventLoop::current();
            $this->c->recvWaiters[] = $loop->currentTask();
            $loop->park();
        }
    }

    /** Stops new sends; values already queued can still be received. */
    public function close(): void
    {
        $this->c->rxClosed = true;
        MpscChannel::wakeAll($this->c->sendWaiters);
        MpscChannel::wakeAll($this->c->recvWaiters);
    }

    /** Number of queued values. */
    public function count(): int
    {
        return $this->c->queue->count();
    }

    /** Yields values until the channel is closed and empty. */
    public function getIterator(): \Generator
    {
        while (true) {
            try {
                yield $this->recv();
            } catch (AsyncException $e) {
                if ($e->errorCode === AsyncException::CLOSED) {
                    return;
                }
                throw $e;
            }
        }
    }
}
