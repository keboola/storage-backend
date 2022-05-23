<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse;

use Doctrine\DBAL\Exception as DBALException;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Exception\ImportExportException;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;
use Throwable;

final class SynapseException extends ImportExportException
{
    public const CODE_TABLE_COLUMNS_MISMATCH = 1501;

    private const BULK_LOAD_EXCEPTION_BEGINNING = '[SQL Server]Bulk load';
    private const DATA_TYPE_CONVERSION_EXCEPTION_BEGINNING = '[SQL Server]Error converting data type';

    public static function createColumnsCountMismatch(
        ColumnCollection $source,
        ColumnCollection $destination
    ): Throwable {
        $columnsSource = array_map(
            static fn(ColumnInterface $col) => $col->getColumnName(),
            iterator_to_array($source->getIterator())
        );
        $columnsDestination = array_map(
            static fn(ColumnInterface $col) => $col->getColumnName(),
            iterator_to_array($destination->getIterator())
        );
        return new self(
            sprintf(
                'Tables don\'t have same number of columns. Source columns: "%s", Destination columns: "%s"',
                implode(',', $columnsSource),
                implode(',', $columnsDestination)
            ),
            self::CODE_TABLE_COLUMNS_MISMATCH
        );
    }

    public static function createColumnsNamesMismatch(
        ColumnInterface $sourceDef,
        ColumnInterface $destDef
    ): Throwable {
        return new self(sprintf(
            'Source destination columns name mismatch. "%s"->"%s"',
            $sourceDef->getColumnName(),
            $destDef->getColumnName()
        ), self::CODE_TABLE_COLUMNS_MISMATCH);
    }

    public static function createColumnsMismatch(
        ColumnInterface $sourceDef,
        ColumnInterface $destDef
    ): Throwable {
        return new self(sprintf(
            'Source destination columns mismatch. "%s %s"->"%s %s"',
            $sourceDef->getColumnName(),
            $sourceDef->getColumnDefinition()->getSQLDefinition(),
            $destDef->getColumnName(),
            $destDef->getColumnDefinition()->getSQLDefinition()
        ), self::CODE_TABLE_COLUMNS_MISMATCH);
    }

    public static function covertException(DBALException $e): Throwable
    {
        if (strpos($e->getMessage(), self::BULK_LOAD_EXCEPTION_BEGINNING) !== false) {
            // - these are errors which appear during COPY INTO
            // - Bulk load data conversion error (when cell has more than 4000chars)
            // - Bulk load failed due to (parsing err in CSV)
            // - possibly something else

            // strip query from message, there are things like SAS tokens and internal table names
            $message = (string) strstr($e->getMessage(), self::BULK_LOAD_EXCEPTION_BEGINNING);
            return new self(
                $message,
                Exception::UNKNOWN_ERROR
            );
        }

        if (strpos($e->getMessage(), self::DATA_TYPE_CONVERSION_EXCEPTION_BEGINNING) !== false) {
            // strip query from message, there are things like SAS tokens and internal table names
            $message = (string) strstr($e->getMessage(), self::DATA_TYPE_CONVERSION_EXCEPTION_BEGINNING);
            return new self(
                $message,
                Exception::UNKNOWN_ERROR
            );
        }

        return $e;
    }
}
