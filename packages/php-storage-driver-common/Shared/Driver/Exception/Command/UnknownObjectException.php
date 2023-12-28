<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\Driver\Exception\Command;

use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

final class UnknownObjectException extends Exception implements
    NonRetryableExceptionInterface
{
    public function __construct(string $objectName, int $code = self::ERR_UNKNOWN_OBJECT, ?Throwable $previous = null)
    {
        parent::__construct(
            sprintf(
                'Unknown object "%s".',
                $objectName,
            ),
            $code,
            $previous,
        );
    }
}
