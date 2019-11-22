<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Snowflake;

use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\Db\ImportExport\Storage\SourceInterface;

class SnowflakeImportAdapter implements BackendImportAdapterInterface
{
    /**
     * @var Table
     */
    private $source;

    /**
     * @param Table $source
     */
    public function __construct(SourceInterface $source)
    {
        $this->source = $source;
    }

    /**
     * @param Table $destination
     */
    public function getCopyCommands(
        DestinationInterface $destination,
        ImportOptions $importOptions,
        string $stagingTableName
    ): array {
        $quotedColumns = array_map(function ($column) {
            return QuoteHelper::quoteIdentifier($column);
        }, $importOptions->getColumns());

        $sql = sprintf(
            'INSERT INTO %s.%s (%s)',
            QuoteHelper::quoteIdentifier($destination->getSchema()),
            QuoteHelper::quoteIdentifier($stagingTableName),
            implode(', ', $quotedColumns)
        );

        $sql .= sprintf(
            ' SELECT %s FROM %s.%s',
            implode(', ', $quotedColumns),
            QuoteHelper::quoteIdentifier($this->source->getSchema()),
            QuoteHelper::quoteIdentifier($this->source->getTableName()),
        );

        return [$sql];
    }
}
