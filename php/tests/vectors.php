<?php

declare(strict_types=1);

// Runs the shared cross-language vectors (vectors/lombokasync-vectors-v1.json).

require_once __DIR__ . '/bootstrap.php';

use LombokAsync\Async;
use LombokAsync\AsyncException;
use LombokAsync\Channel\MpscChannel;
use LombokAsync\Channel\OneshotChannel;
use LombokAsync\Executor\EventLoop;

$doc = json_decode((string) file_get_contents(__DIR__ . '/../../vectors/lombokasync-vectors-v1.json'), true, 512, JSON_THROW_ON_ERROR);

function outcome(AsyncException $e): string
{
    return strtolower($e->errorCode);
}

function task(array $spec): callable
{
    return static function () use ($spec): mixed {
        if (!empty($spec['never'])) {
            while (true) {
                EventLoop::current()->park();
            }
        }
        for ($i = 0; $i < ($spec['yields'] ?? 0); $i++) {
            Async::yield();
        }
        if (array_key_exists('error', $spec)) {
            throw new \RuntimeException($spec['error']);
        }
        return $spec['value'];
    };
}

function runMpsc(array $ops): array
{
    $out = [];
    $senders = [];
    $rx = null;
    foreach ($ops as $op) {
        switch ($op[0]) {
            case 'new':
                try {
                    [$tx, $rx] = $op[1] === null ? MpscChannel::unbounded() : MpscChannel::bounded($op[1]);
                } catch (AsyncException $e) {
                    $out[] = outcome($e);
                    return $out;
                }
                $senders[] = $tx;
                $out[] = 'ok';
                break;
            case 'clone':
                $senders[] = $senders[$op[1]]->clone();
                $out[] = ['sender' => count($senders) - 1];
                break;
            case 'send':
                try {
                    $senders[$op[1]]->trySend($op[2]);
                    $out[] = 'ok';
                } catch (AsyncException $e) {
                    $out[] = outcome($e);
                }
                break;
            case 'recv':
                try {
                    $out[] = ['value' => $rx->tryRecv()];
                } catch (AsyncException $e) {
                    $out[] = outcome($e);
                }
                break;
            case 'drop':
                $senders[$op[1]]->close();
                $out[] = 'ok';
                break;
            case 'close':
                $rx->close();
                $out[] = 'ok';
                break;
            case 'len':
                $out[] = ['len' => count($rx)];
                break;
            default:
                throw new \LogicException("unknown op {$op[0]}");
        }
    }
    return $out;
}

function runOneshot(array $ops): array
{
    [$tx, $rx] = OneshotChannel::create();
    $out = [];
    foreach ($ops as $op) {
        try {
            switch ($op[0]) {
                case 'new':
                    $out[] = 'ok';
                    break;
                case 'send':
                    $tx->send($op[1]);
                    $out[] = 'ok';
                    break;
                case 'recv':
                    $out[] = ['value' => $rx->tryRecv()];
                    break;
                case 'drop_tx':
                    $tx->close();
                    $out[] = 'ok';
                    break;
                case 'close':
                    $rx->close();
                    $out[] = 'ok';
                    break;
                default:
                    throw new \LogicException("unknown op {$op[0]}");
            }
        } catch (AsyncException $e) {
            $out[] = outcome($e);
        }
    }
    return $out;
}

function runCase(array $case, int $deadlineMs): mixed
{
    switch ($case['kind']) {
        case 'mpsc':
            return runMpsc($case['ops']);
        case 'oneshot':
            return runOneshot($case['ops']);
    }
    return Async::run(static function () use ($case, $deadlineMs): mixed {
        try {
            switch ($case['kind']) {
                case 'join_all':
                    return ['ok' => Async::joinAll(array_map('task', $case['tasks']))];
                case 'select':
                    [$i, $v] = Async::select(array_map('task', $case['tasks']));
                    return ['index' => $i, 'value' => $v];
                case 'timeout':
                    try {
                        return ['ok' => Async::timeout($deadlineMs, task($case['task']))];
                    } catch (AsyncException $e) {
                        return $e->errorCode === AsyncException::TIMEOUT ? 'timeout' : throw $e;
                    }
            }
        } catch (\RuntimeException $e) {
            return ['error' => $e->getMessage()];
        }
        throw new \LogicException("unknown kind {$case['kind']}");
    });
}

$flags = JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES;
check(count($doc['cases']) >= 100, 'at least 100 cases');
foreach ($doc['cases'] as $case) {
    $got = json_encode(runCase($case, $doc['timeout_deadline_ms']), $flags);
    $want = json_encode($case['expected'], $flags);
    check($got === $want, "{$case['id']}: got $got want $want");
}
finish('vectors');
