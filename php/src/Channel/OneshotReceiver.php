<?php

declare(strict_types=1);

namespace LombokAsync\Channel;

use LombokAsync\AsyncException;
use LombokAsync\Executor\EventLoop;

/** Receiving half of a oneshot channel. */
final class OneshotReceiver
{
    /** @internal */
    public function __construct(private readonly OneshotChannel $c)
    {
    }

    /**
     * Takes the value without waiting.
     *
     * @throws AsyncException EMPTY when no value yet, CLOSED when none can arrive
     */
    public function tryRecv(): mixed
    {
        $c = $this->c;
        if ($c->hasValue && !$c->taken) {
            $c->taken = true;
            $value = $c->value;
            $c->value = null;
            return $value;
        }
        if ($c->taken || $c->rxClosed || $c->txDropped) {
            throw new AsyncException(AsyncException::CLOSED, 'channel is closed');
        }
        throw new AsyncException(AsyncException::EMPTY, 'no value yet');
    }

    /**
     * Waits (inside a task) for the value.
     *
     * @throws AsyncException CLOSED when none can arrive
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
            $this->c->waiters[] = $loop->currentTask();
            $loop->park();
        }
    }

    /** Stops the sender from sending; a value sent earlier can still be taken. */
    public function close(): void
    {
        $this->c->rxClosed = true;
    }
}
