<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse\Exception;

use Keboola\Db\ImportExport\Backend\Synapse\DestinationTableOptions;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage\ABS\SourceFile;
use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\Synapse\Table;
use Keboola\Db\Import\Exception;

final class Assert
{
    public static function assertColumns(
        SourceInterface $source,
        DestinationTableOptions $destinationTableOptions
    ): void {
        if (count($source->getColumnsNames()) === 0) {
            throw new Exception(
                'No columns found in CSV file.',
                Exception::NO_COLUMNS
            );
        }

        $moreColumns = array_diff($source->getColumnsNames(), $destinationTableOptions->getColumnNamesInOrder());
        if (!empty($moreColumns)) {
            throw new Exception(
                'Columns doest not match. Non existing columns: ' . implode(', ', $moreColumns),
                Exception::COLUMNS_COUNT_NOT_MATCH
            );
        }
    }

    public static function assertIsSynapseTableDestination(DestinationInterface $destination): void
    {
        if (!$destination instanceof Table) {
            throw new \Exception(sprintf(
                'Only "%s" is supported as destination "%s" provided.',
                Table::class,
                get_class($destination)
            ));
        }
    }

    public static function assertSynapseImportOptions(ImportOptionsInterface $options): void
    {
        if (!$options instanceof SynapseImportOptions) {
            throw new \Exception(sprintf(
                'Synapse importer expect $options to be instance of "%s", "%s" given.',
                SynapseImportOptions::class,
                get_class($options)
            ));
        }
    }

    public static function assertValidSource(SourceInterface $source): void
    {
        if ($source instanceof SourceFile
            && $source->getCsvOptions()->getEnclosure() === ''
        ) {
            throw new \Exception(
                'CSV property FIELDQUOTE|ECLOSURE must be set when using Synapse analytics.'
            );
        }
    }
}
