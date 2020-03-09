<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Synapse;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\SQLServerPlatform;
use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\Db\ImportExport\Storage\SourceInterface;

class SynapseImportAdapter implements BackendImportAdapterInterface
{
    /**
     * @var Table
     */
    private $source;

    /** @var SQLServerPlatform */
    private $platform;

    /**
     * @param Table $source
     */
    public function __construct(SourceInterface $source, SQLServerPlatform $platform)
    {
        $this->source = $source;
        $this->platform = $platform;
    }

    /**
     * phpcs:disable SlevomatCodingStandard.TypeHints.TypeHintDeclaration.MissingParameterTypeHint
     * @param Table $destination
     * @param Connection $connection
     */
    public function getCopyCommands(
        DestinationInterface $destination,
        ImportOptions $importOptions,
        string $stagingTableName,
        $connection = null
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
            $this->platform->quoteSingleIdentifier($this->source->getSchema()),
            $this->platform->quoteSingleIdentifier($this->source->getTableName())
        );

        return [$sql];
    }
}
