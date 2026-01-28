<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Handler\Table;

use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

class TableExportFailedException extends Exception
{
    public function __construct(
        string $message,
        int $code = self::ERR_UNKNOWN_EXPORT,
        ?Throwable $previous = null,
    ) {
        parent::__construct(
            $message,
            $code,
            $previous,
        );
    }
}
