<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Snowflake;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\SqlCommandBuilder;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;

class SnowflakeImportAdapter implements SnowflakeImportAdapterInterface
{
    /** @var Connection */
    private $connection;

    /** @var SqlCommandBuilder */
    private $sqlBuilder;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        $this->sqlBuilder = new SqlCommandBuilder();
    }

    public static function isSupported(Storage\SourceInterface $source, Storage\DestinationInterface $destination): bool
    {
        if (!$destination instanceof Table) {
            return false;
        }

        if (!$source instanceof Table && !$source instanceof SelectSource) {
            return false;
        }

        return true;
    }

    /**
     * @param Table|SelectSource $source
     * @param Table $destination
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptionsInterface $importOptions,
        string $stagingTableName
    ): int {
        $quotedColumns = array_map(function ($column) {
            return $this->connection->quoteIdentifier($column);
        }, $source->getColumnsNames());

        $sql = sprintf(
            'INSERT INTO %s.%s (%s) %s',
            $this->connection->quoteIdentifier($destination->getSchema()),
            $this->connection->quoteIdentifier($stagingTableName),
            implode(', ', $quotedColumns),
            $source->getFromStatement()
        );

        $this->connection->query(
            $sql,
            $source instanceof SelectSource ? $source->getQueryBindings() : []
        );

        $rows = $this->connection->fetchAllAssociative($this->sqlBuilder->getTableItemsCountCommand(
            $destination->getSchema(),
            $stagingTableName
        ));

        return (int) $rows[0]['count'];
    }
}
