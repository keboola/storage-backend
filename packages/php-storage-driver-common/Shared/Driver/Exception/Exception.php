<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\Driver\Exception;

use Exception as NativeException;
use Keboola\StorageDriver\Contract\Driver\Exception\ExceptionInterface;
use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;

class Exception extends NativeException implements ExceptionInterface
{
    /** @var mixed[] */
    private array $context = [];

    public function isRetryable(): bool
    {
        return !$this instanceof NonRetryableExceptionInterface;
    }

    /**
     * @param mixed[] $context
     */
    public function setContext(array $context): void
    {
        $this->context = $context;
    }

    /**
     * @return mixed[]
     */
    public function getContext(): array
    {
        return $this->context;
    }
}
