<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\Driver\Exception\Command;

use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

final class InvalidObjectNameException extends Exception implements
    NonRetryableExceptionInterface
{
    public function __construct(string $objectName, int $code = self::ERR_VALIDATION, ?Throwable $previous = null)
    {
        parent::__construct(
            sprintf(
                'Name "%s" is not valid name for user name in Snowflake DB.',
                $objectName,
            ),
            $code,
            $previous,
        );
    }
}
