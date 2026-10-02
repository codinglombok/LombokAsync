<?php

declare(strict_types=1);

namespace LombokAsync\Channel;

use LombokAsync\AsyncException;
use LombokAsync\Executor\EventLoop;
use LombokAsync\Executor\Task;

/**
 * Multi-producer, single-consumer channel (SPEC section 3). Create one with
 * {@see unbounded()} or {@see bounded()}; this object is the shared state.
 *
 * @template T
 */
final class MpscChannel
{
    /** @var \SplQueue<T> @internal */
    public \SplQueue $queue;
    /** @internal */
    public int $senders = 0;
    /** @internal */
    public bool $rxClosed = false;
    /** @var list<Task> @internal */
    public array $recvWaiters = [];
    /** @var list<Task> @internal */
    public array $sendWaiters = [];

    private function __construct(public readonly ?int $capacity)
    {
        $this->queue = new \SplQueue();
    }

    /**
     * Creates an unbounded channel.
     *
     * @return array{0: MpscSender, 1: MpscReceiver}
     */
    public static function unbounded(): array
    {
        $c = new self(null);
        return [new MpscSender($c), new MpscReceiver($c)];
    }

    /**
     * Creates a channel that holds at most $capacity values.
     *
     * @return array{0: MpscSender, 1: MpscReceiver}
     * @throws AsyncException INVALID_CAPACITY when $capacity < 1
     */
    public static function bounded(int $capacity): array
    {
        if ($capacity < 1) {
            throw new AsyncException(AsyncException::INVALID_CAPACITY, 'capacity must be at least 1');
        }
        $c = new self($capacity);
        return [new MpscSender($c), new MpscReceiver($c)];
    }

    /** @deprecated 0.1 name; use {@see unbounded()}. */
    public static function create(): array
    {
        return self::unbounded();
    }

    /**
     * @internal
     * @param list<Task> $waiters
     */
    public static function wakeAll(array &$waiters): void
    {
        $list = $waiters;
        $waiters = [];
        if ($list !== []) {
            $loop = EventLoop::current();
            foreach ($list as $t) {
                $loop->wake($t);
            }
        }
    }
}
