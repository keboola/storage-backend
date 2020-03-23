<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Synapse;

use Doctrine\DBAL\Platforms\SQLServerPlatform;
use Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface;
use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\Synapse\Importer as SynapseImporter;
use Keboola\Db\ImportExport\Backend\ExporterInterface;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\Db\ImportExport\Storage\NoBackendAdapterException;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;

class Table implements SourceInterface, DestinationInterface, SqlSourceInterface
{
    /**
     * @var string
     */
    private $schema;

    /**
     * @var string
     */
    private $tableName;

    /** @var SQLServerPlatform */
    private $platform;

    public function __construct(string $schema, string $tableName)
    {
        $this->schema = $schema;
        $this->tableName = $tableName;
        $this->platform = new SQLServerPlatform();
    }

    public function getFromStatement(): string
    {
        return sprintf('SELECT * FROM %s', $this->getQuotedTableWithScheme());
    }

    public function getQuotedTableWithScheme(): string
    {
        return sprintf(
            '%s.%s',
            $this->platform->quoteSingleIdentifier($this->schema),
            $this->platform->quoteSingleIdentifier($this->tableName)
        );
    }

    public function getSchema(): string
    {
        return $this->schema;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }

    public function getQueryBindings(): array
    {
        return [];
    }
}
