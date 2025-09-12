<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Handler\Bucket;

use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

class BucketCreationFailedException extends Exception
{
    public function __construct(
        string $message,
        int $code = self::ERR_VALIDATION,
        ?Throwable $previous = null,
    ) {
        parent::__construct(
            $message,
            $code,
            $previous,
        );
    }
}
