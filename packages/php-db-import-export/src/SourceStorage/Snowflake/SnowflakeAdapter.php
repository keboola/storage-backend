<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\SourceStorage\Snowflake;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportAdapterInterface;
use Keboola\Db\ImportExport\SourceStorage\SourceInterface;

class SnowflakeAdapter implements SnowflakeImportAdapterInterface
{
    /**
     * @var Source
     */
    private $source;

    /**
     * @param Source $source
     */
    public function __construct(SourceInterface $source)
    {
        $this->source = $source;
    }

    /**
     * @inheritDoc
     */
    public function executeCopyCommands(
        array $commands,
        Connection $connection,
        ImportOptions $importOptions,
        ImportState $importState
    ): int {
        $importState->startTimer('copyToStaging');
        $connection->query($commands[0]);
        $rows = $connection->fetchAll(sprintf(
            'SELECT COUNT(*) AS "count" FROM %s.%s',
            QuoteHelper::quoteIdentifier($importOptions->getSchema()),
            QuoteHelper::quoteIdentifier($importState->getStagingTableName())
        ));
        $importState->stopTimer('copyToStaging');
        return (int) $rows[0]['count'];
    }

    public function getCopyCommands(
        ImportOptions $importOptions,
        string $stagingTableName
    ): array {
        $quotedColumns = array_map(function ($column) {
            return QuoteHelper::quoteIdentifier($column);
        }, $importOptions->getColumns());

        $sql = sprintf(
            'INSERT INTO %s.%s (%s)',
            QuoteHelper::quoteIdentifier($importOptions->getSchema()),
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
