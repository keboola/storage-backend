<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Snowflake;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportAdapterInterface;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;

class SnowflakeImportAdapter implements SnowflakeImportAdapterInterface
{
    /** @var Connection */
    private $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public static function isSupported(Storage\SourceInterface $source, Storage\DestinationInterface $destination): bool
    {
        if (!$source instanceof Storage\Snowflake\Table) {
            return false;
        }
        if (!$destination instanceof Storage\Snowflake\Table) {
            return false;
        }
        return true;
    }

    /**
     * @param Storage\Snowflake\Table $source
     * @param Storage\Snowflake\Table $destination
     */
    public function getCopyCommands(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptions $importOptions,
        string $stagingTableName
    ): array {
        $quotedColumns = array_map(function ($column) {
            return $this->connection->quoteIdentifier($column);
        }, $importOptions->getColumns());

        $sql = sprintf(
            'INSERT INTO %s.%s (%s)',
            $this->connection->quoteIdentifier($destination->getSchema()),
            $this->connection->quoteIdentifier($stagingTableName),
            implode(', ', $quotedColumns)
        );

        $sql .= sprintf(
            ' SELECT %s FROM %s.%s',
            implode(', ', $quotedColumns),
            $this->connection->quoteIdentifier($source->getSchema()),
            $this->connection->quoteIdentifier($source->getTableName())
        );

        return [$sql];
    }
}
