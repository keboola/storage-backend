<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver\Exception;

interface NonRetryableExceptionInterface extends ExceptionInterface
{
    /**
     * @return false
     */
    public function isRetryable(): bool;
}
