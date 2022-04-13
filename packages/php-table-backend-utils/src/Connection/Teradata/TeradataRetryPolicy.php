<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Teradata;

use Retry\Policy\AbstractRetryPolicy;
use Retry\RetryContextInterface;

class TeradataRetryPolicy extends AbstractRetryPolicy
{
    private const PATTERNS = [
        'Concurrent change conflict on database -- try again',
        'Connection reset by peer',
    ];
    public function canRetry(RetryContextInterface $context): bool
    {
        $e = $context->getLastException();
        if ($e === null) {
            return $context->getRetryCount() === 0;
        }

        foreach (self::PATTERNS as $pattern){
            $pattern = '/'.$pattern.'/';
            $matches = null;
            if (preg_match($pattern, $e->getMessage(), $matches)) {
                return true;
            }
        }

        return false;
    }
}
