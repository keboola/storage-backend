<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Synapse;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\SQLServerPlatform;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportAdapterInterface;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;

class SynapseImportAdapter implements SynapseImportAdapterInterface
{
    /** @var \Doctrine\DBAL\Platforms\AbstractPlatform|SQLServerPlatform */
    private $platform;

    public function __construct(Connection $connection)
    {
        $this->platform = $connection->getDatabasePlatform();
    }

    public static function isSupported(Storage\SourceInterface $source, Storage\DestinationInterface $destination): bool
    {
        if (!$source instanceof Storage\Synapse\Table) {
            return false;
        }
        if (!$destination instanceof Storage\Synapse\Table) {
            return false;
        }
        return true;
    }

    /**
     * @param Storage\Synapse\Table $source
     * @param Storage\Synapse\Table $destination
     */
    public function getCopyCommands(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptions $importOptions,
        string $stagingTableName
    ): array {
        $quotedColumns = array_map(function ($column) {
            return $this->platform->quoteSingleIdentifier($column);
        }, $importOptions->getColumns());

        $sql = sprintf(
            'INSERT INTO %s.%s (%s)',
            $this->platform->quoteSingleIdentifier($destination->getSchema()),
            $this->platform->quoteSingleIdentifier($stagingTableName),
            implode(', ', $quotedColumns)
        );

        $sql .= sprintf(
            ' SELECT %s FROM %s.%s',
            implode(', ', $quotedColumns),
            $this->platform->quoteSingleIdentifier($source->getSchema()),
            $this->platform->quoteSingleIdentifier($source->getTableName())
        );

        return [$sql];
    }
}
