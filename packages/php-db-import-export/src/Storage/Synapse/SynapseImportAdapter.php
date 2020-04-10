<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Synapse;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\SQLServerPlatform;
use Keboola\Db\ImportExport\Backend\Synapse\SqlCommandBuilder;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportAdapterInterface;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;

class SynapseImportAdapter implements SynapseImportAdapterInterface
{
    /** @var \Doctrine\DBAL\Platforms\AbstractPlatform|SQLServerPlatform */
    private $platform;

    /** @var Connection */
    private $connection;

    /** @var SqlCommandBuilder */
    private $sqlBuilder;

    public function __construct(Connection $connection)
    {
        $this->platform = $connection->getDatabasePlatform();
        $this->connection = $connection;
        $this->sqlBuilder = new SqlCommandBuilder($this->connection);
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
        ImportOptions $importOptions,
        string $stagingTableName
    ): int {
        $quotedColumns = array_map(function ($column) {
            return $this->platform->quoteSingleIdentifier($column);
        }, $source->getColumnsNames());

        $sql = sprintf(
            'INSERT INTO %s.%s (%s) %s',
            $this->platform->quoteSingleIdentifier($destination->getSchema()),
            $this->platform->quoteSingleIdentifier($stagingTableName),
            implode(', ', $quotedColumns),
            $source->getFromStatement()
        );

        if ($source instanceof SelectSource) {
            $this->connection->executeQuery($sql, $source->getQueryBindings(), $source->getDataTypes());
        } else {
            $this->connection->exec($sql);
        }

        $rows = $this->connection->fetchAll($this->sqlBuilder->getTableItemsCountCommand(
            $destination->getSchema(),
            $stagingTableName
        ));

        return (int) $rows[0]['count'];
    }
}
