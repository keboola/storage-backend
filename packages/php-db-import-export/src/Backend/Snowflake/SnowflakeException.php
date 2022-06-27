<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Storage\FileNotFoundException;
use Throwable;

class SnowflakeException extends Exception
{
    private const MANIFEST_ENTRY_NOT_FOUND = 'resulted in a `404 Not Found` response';

    public static function covertException(Throwable $e): Throwable
    {
        if (strpos($e->getMessage(), self::MANIFEST_ENTRY_NOT_FOUND) !== false) {
            return new FileNotFoundException($e->getMessage());
        }

        return $e;
    }
}
