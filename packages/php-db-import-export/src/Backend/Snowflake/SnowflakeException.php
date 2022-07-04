<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Storage\FileNotFoundException;
use Throwable;

class SnowflakeException extends Exception
{
    private const CONSTRAINT_VIOLATION_NOT_NULL =
        'An exception occurred while executing a query: NULL result in a non-nullable column';

    public static function covertException(Throwable $e): Throwable
    {
        // file on S3 not found
        if (preg_match('/Remote file \'.+\' was not found/', $e->getMessage(), $output_array) === 1) {
            return new FileNotFoundException($e->getMessage());
        }

        if ($e->getMessage() === self::CONSTRAINT_VIOLATION_NOT_NULL) {
            // strip query from message, there are things like internal table names

            return new Exception(
                'Load error: ' . $e->getMessage(),
                Exception::UNKNOWN_ERROR,
                $e
            );
        }

        return $e;
    }
}
