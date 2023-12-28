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
                $e,
            );
        }

        // phpcs:ignore
        $message = $e->getMessage();
        $isObjectCastFail = preg_match('/Failed to cast variant value .* to OBJECT/', $message, $output_array) === 1;
        if ($isObjectCastFail) {
            // remove variant from message as it would confuse users
            // we are using TO_OBJECT(TO_VARIANT(...)) casting combination
            $message = str_replace('variant ', '', $message);
        }
        $isInvalidGeo = preg_match('/Error parsing Geo input/', $message, $output_array) === 1;
        // phpcs:ignore
        $isInvalidBinary = preg_match('/The following string is not a legal hex-encoded value/', $message, $output_array) === 1;
        $isNullInNotNullCol = preg_match('/NULL result in a non-nullable column/', $message, $output_array) === 1;
        $isNotRecognized = preg_match('/ \'(.*)\' is not recognized/', $message, $output_array) === 1;
        if ($isNotRecognized) {
            $message .= '. Value you are trying to load cannot be converted to used datatype.';
        }
        if ($isNullInNotNullCol || $isNotRecognized || $isInvalidBinary || $isInvalidGeo || $isObjectCastFail) {
            return new Exception(
                'Load error: ' . $message,
                Exception::VALUE_CONVERSION,
                $e,
            );
        }

        $isOutOfRange = preg_match('/.*out of range/', $e->getMessage(), $output_array) === 1;
        // phpcs:ignore
        $isBiggerThenColumnSize = preg_match('/ \'(.*)\' cannot be inserted because it\'s bigger than column size/', $e->getMessage(), $output_array) === 1;

        if ($isBiggerThenColumnSize || $isOutOfRange) {
            return new Exception(
                'Load error: ' . $e->getMessage(),
                Exception::ROW_SIZE_TOO_LARGE,
                $e,
            );
        }

        return $e;
    }
}
