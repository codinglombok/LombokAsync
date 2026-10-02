<?php

declare(strict_types=1);

namespace LombokAsync\Channel;

use LombokAsync\Executor\Task;

/**
 * Oneshot channel (SPEC section 4): at most one value. This object is the
 * shared state; create a pair with {@see create()}.
 */
final class OneshotChannel
{
    /** @internal */
    public mixed $value = null;
    /** @internal */
    public bool $hasValue = false;
    /** @internal */
    public bool $txUsed = false;
    /** @internal */
    public bool $txDropped = false;
    /** @internal */
    public bool $rxClosed = false;
    /** @internal */
    public bool $taken = false;
    /** @var list<Task> @internal */
    public array $waiters = [];

    /**
     * @return array{0: OneshotSender, 1: OneshotReceiver}
     */
    public static function create(): array
    {
        $c = new self();
        return [new OneshotSender($c), new OneshotReceiver($c)];
    }
}
