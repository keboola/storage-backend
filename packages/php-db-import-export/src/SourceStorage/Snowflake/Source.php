<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\SourceStorage\Snowflake;

use Keboola\Db\ImportExport\SourceStorage\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\SourceStorage\Snowflake\SnowflakeAdapter;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer as SnowflakeImporter;
use Keboola\Db\ImportExport\SourceStorage\NoBackendAdapterException;
use Keboola\Db\ImportExport\SourceStorage\SourceInterface;

class Source implements SourceInterface
{
    /**
     * @var string
     */
    private $schema;

    /**
     * @var string
     */
    private $tableName;

    public function __construct(string $schema, string $tableName)
    {
        $this->schema = $schema;
        $this->tableName = $tableName;
    }

    public function getBackendAdapter(
        ImporterInterface $importer
    ): BackendImportAdapterInterface {
        switch (true) {
            case $importer instanceof SnowflakeImporter:
                return new SnowflakeAdapter($this);
            default:
                throw new NoBackendAdapterException();
        }
    }

    public function getSchema(): string
    {
        return $this->schema;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }
}
