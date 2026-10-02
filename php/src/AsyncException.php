<?php

declare(strict_types=1);

namespace LombokAsync;

/**
 * Error with a code shared by every LombokAsync port (SPEC section 2).
 * PHP-only code: CANCELLED (awaiting a cancelled task).
 */
final class AsyncException extends \RuntimeException
{
    public const TIMEOUT = 'TIMEOUT';
    public const CLOSED = 'CLOSED';
    public const FULL = 'FULL';
    public const EMPTY = 'EMPTY';
    public const ALREADY_SENT = 'ALREADY_SENT';
    public const INVALID_CAPACITY = 'INVALID_CAPACITY';
    public const CANCELLED = 'CANCELLED';

    public function __construct(public readonly string $errorCode, string $message)
    {
        parent::__construct($errorCode . ': ' . $message);
    }
}
