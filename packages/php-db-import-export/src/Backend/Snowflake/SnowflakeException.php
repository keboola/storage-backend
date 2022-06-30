<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Storage\FileNotFoundException;
use Throwable;

class SnowflakeException extends Exception
{
    public static function covertException(Throwable $e): Throwable
    {
        // file on S3 not found
        if (preg_match('/Remote file \'.+\' was not found/', $e->getMessage(), $output_array) === 1) {
            return new FileNotFoundException($e->getMessage());
        }

        return $e;
    }
}
