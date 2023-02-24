<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Exception;

use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;

class ColumnsMismatchException extends ImportExportException
{
    public static function createColumnsNamesMismatch(
        ColumnInterface $sourceDef,
        ColumnInterface $destDef
    ): ColumnsMismatchException {
        return new self(sprintf(
            'Source destination columns name mismatch. "%s"->"%s"',
            $sourceDef->getColumnName(),
            $destDef->getColumnName()
        ));
    }

    public static function createColumnsMismatch(
        ColumnInterface $sourceDef,
        ColumnInterface $destDef
    ): ColumnsMismatchException {
        return new self(sprintf(
            'Source destination columns mismatch. "%s %s"->"%s %s"',
            $sourceDef->getColumnName(),
            $sourceDef->getColumnDefinition()->getSQLDefinition(),
            $destDef->getColumnName(),
            $destDef->getColumnDefinition()->getSQLDefinition()
        ));
    }

    public static function createColumnsCountMismatch(
        ColumnCollection $source,
        ColumnCollection $destination
    ): ColumnsMismatchException {
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
            )
        );
    }
}
