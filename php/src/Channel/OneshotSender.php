<?php

declare(strict_types=1);

namespace LombokAsync\Channel;

use LombokAsync\AsyncException;

/** Sending half of a oneshot channel. */
final class OneshotSender
{
    /** @internal */
    public function __construct(private readonly OneshotChannel $c)
    {
    }

    /**
     * Sends the value. Any attempt uses the sender up.
     *
     * @throws AsyncException ALREADY_SENT on a second attempt, CLOSED when the receiver is closed
     */
    public function send(mixed $value): void
    {
        $c = $this->c;
        if ($c->txUsed) {
            throw new AsyncException(AsyncException::ALREADY_SENT, 'oneshot sender was already used');
        }
        $c->txUsed = true;
        if ($c->rxClosed) {
            throw new AsyncException(AsyncException::CLOSED, 'channel is closed');
        }
        $c->value = $value;
        $c->hasValue = true;
        MpscChannel::wakeAll($c->waiters);
    }

    /** Drops the sender without sending. Idempotent. */
    public function close(): void
    {
        $this->c->txDropped = true;
        MpscChannel::wakeAll($this->c->waiters);
    }

    /** True when the receiver is closed. */
    public function isClosed(): bool
    {
        return $this->c->rxClosed;
    }
}
