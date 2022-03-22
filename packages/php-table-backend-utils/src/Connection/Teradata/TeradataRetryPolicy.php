<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Teradata;

use Retry\Policy\AbstractRetryPolicy;
use Retry\RetryContextInterface;

class TeradataRetryPolicy extends AbstractRetryPolicy
{
    public function canRetry(RetryContextInterface $context): bool
    {
        $e = $context->getLastException();
        if ($e === null) {
            return false;
        }

        $pattern = '/Concurrent change conflict on database -- try again/';
        $matches = null;
        if (preg_match($pattern, $e->getMessage(), $matches)) {
            return true;
        }

        return false;
    }
}
