<?php

declare(strict_types=1);

namespace LombokAsync\Channel;

use LombokAsync\Executor\EventLoop;

/**
 * Oneshot channel — send exactly one value.
 */
class OneshotChannel
{
    private mixed $value = null;
    private bool $sent = false;

    /**
     * Create a oneshot channel and return [sender, receiver].
     *
     * @return array{0: OneshotSender, 1: OneshotReceiver}
     */
    public static function create(): array
    {
        $channel = new self();
        return [new OneshotSender($channel), new OneshotReceiver($channel)];
    }

    /** @internal */
    public function send(mixed $value): void
    {
        if ($this->sent) {
            throw new \RuntimeException('oneshot already sent');
        }
        $this->value = $value;
        $this->sent = true;
    }

    /** @internal */
    public function recv(): mixed
    {
        return $this->value;
    }

    public function isSent(): bool
    {
        return $this->sent;
    }
}

class OneshotSender
{
    public function __construct(private OneshotChannel $channel) {}

    public function send(mixed $value): void
    {
        $this->channel->send($value);
    }
}

class OneshotReceiver
{
    public function __construct(private OneshotChannel $channel) {}

    /** Returns the value (non-blocking). */
    public function recv(): mixed
    {
        return $this->channel->recv();
    }

    /** Wait cooperatively until a value is sent. */
    public function recvBlocking(): mixed
    {
        while (!$this->channel->isSent()) {
            EventLoop::yield();
        }
        return $this->channel->recv();
    }
}
