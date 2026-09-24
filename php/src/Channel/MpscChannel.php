<?php

declare(strict_types=1);

namespace LombokAsync\Channel;

use LombokAsync\Executor\EventLoop;

/**
 * Multi-producer, single-consumer channel using SplQueue.
 */
class MpscChannel
{
    private \SplQueue $queue;
    private bool $closed = false;

    public function __construct()
    {
        $this->queue = new \SplQueue();
    }

    /**
     * Create a new mpsc channel and return [sender, receiver].
     *
     * @return array{0: MpscSender, 1: MpscReceiver}
     */
    public static function create(): array
    {
        $channel = new self();
        return [new MpscSender($channel), new MpscReceiver($channel)];
    }

    /** @internal */
    public function send(mixed $value): void
    {
        if ($this->closed) {
            throw new \RuntimeException('channel closed');
        }
        $this->queue->enqueue($value);
    }

    /** @internal */
    public function recv(): array
    {
        if (!$this->queue->isEmpty()) {
            return [$this->queue->dequeue(), true];
        }
        if ($this->closed) {
            return [null, false];
        }
        return [null, false]; // non-blocking: empty
    }

    /** @internal */
    public function recvBlocking(): array
    {
        // Wait cooperatively
        while ($this->queue->isEmpty() && !$this->closed) {
            EventLoop::yield();
        }
        return $this->recv();
    }

    /** @internal */
    public function close(): void
    {
        $this->closed = true;
    }

    public function isClosed(): bool
    {
        return $this->closed;
    }

    public function isEmpty(): bool
    {
        return $this->queue->isEmpty();
    }
}

class MpscSender
{
    public function __construct(private MpscChannel $channel) {}

    public function send(mixed $value): void
    {
        $this->channel->send($value);
    }

    public function close(): void
    {
        $this->channel->close();
    }
}

class MpscReceiver
{
    public function __construct(private MpscChannel $channel) {}

    /** Non-blocking receive. Returns [value, true] or [null, false]. */
    public function recv(): array
    {
        return $this->channel->recv();
    }

    /** Blocking receive (cooperative). Returns [value, true] or [null, false]. */
    public function recvBlocking(): array
    {
        return $this->channel->recvBlocking();
    }
}
