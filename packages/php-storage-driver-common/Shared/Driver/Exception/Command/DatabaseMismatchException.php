<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\Driver\Exception\Command;

use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

final class DatabaseMismatchException extends Exception implements NonRetryableExceptionInterface
{
    public function __construct(
        string $requested,
        string $connected,
        ?Throwable $previous = null,
    ) {
        parent::__construct(
            sprintf(
                'Requested database "%s" does not match the connected database "%s".',
                $requested,
                $connected,
            ),
            self::ERR_DATABASE_MISMATCH,
            $previous,
        );
    }
}
