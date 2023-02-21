<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\Driver\Exception\Command;

use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

final class ObjectNotFoundException extends Exception implements
    NonRetryableExceptionInterface
{
    protected string $objectName;

    public function __construct(string $objectName, int $code = self::ERR_NOT_FOUND, ?Throwable $previous = null)
    {
        $this->objectName = $objectName;
        parent::__construct(sprintf(
            'Object "%s" not found.',
            $objectName
        ), $code, $previous);
    }

    public function getObjectName(): string
    {
        return $this->objectName;
    }
}
