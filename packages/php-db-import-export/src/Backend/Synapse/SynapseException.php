<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse;

use Doctrine\DBAL\Exception as DBALException;
use Keboola\Db\Import\Exception;
use Throwable;

class SynapseException extends Exception
{
    private const BULK_LOAD_EXCEPTION_BEGINNING = '[SQL Server]Bulk load';
    private const DATA_TYPE_CONVERSION_EXCEPTION_BEGINNING = '[SQL Server]Error converting data type';

    public static function covertException(DBALException $e): Throwable
    {
        if (strpos($e->getMessage(), self::BULK_LOAD_EXCEPTION_BEGINNING) !== false) {
            // - these are errors which appear during COPY INTO
            // - Bulk load data conversion error (when cell has more than 4000chars)
            // - Bulk load failed due to (parsing err in CSV)
            // - possibly something else

            // strip query from message, there are things like SAS tokens and internal table names
            $message = (string) strstr($e->getMessage(), self::BULK_LOAD_EXCEPTION_BEGINNING);
            return new Exception(
                $message,
                Exception::UNKNOWN_ERROR
            );
        }

        if (strpos($e->getMessage(), self::DATA_TYPE_CONVERSION_EXCEPTION_BEGINNING) !== false) {
            // strip query from message, there are things like SAS tokens and internal table names
            $message = (string) strstr($e->getMessage(), self::DATA_TYPE_CONVERSION_EXCEPTION_BEGINNING);
            return new Exception(
                $message,
                Exception::UNKNOWN_ERROR
            );
        }

        return $e;
    }
}
