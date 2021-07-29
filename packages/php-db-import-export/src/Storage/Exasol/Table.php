<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Exasol;

use Doctrine\DBAL\Platforms\SQLServerPlatform;
use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;

class Table implements SourceInterface, DestinationInterface, SqlSourceInterface
{
    /** @var string */
    private $schema;

    /** @var string */
    private $tableName;

    /** @var SQLServerPlatform */
    private $platform;

    /** @var string[] */
    private $columnsNames;

    /** @var string[]|null */
    private $primaryKeysNames;

    /**
     * @param string[] $columns
     * @param string[]|null $primaryKeysNames
     */
    public function __construct(
        string $schema,
        string $tableName,
        array $columns = [],
        ?array $primaryKeysNames = null
    ) {
        $this->schema = $schema;
        $this->tableName = $tableName;
        $this->platform = new SQLServerPlatform();
        $this->columnsNames = $columns;
        $this->primaryKeysNames = $primaryKeysNames;
    }

    public function getFromStatement(): string
    {
        $quotedColumns = array_map(function ($column) {
            return $this->platform->quoteSingleIdentifier($column);
        }, $this->getColumnsNames());

        $select = '*';
        if (count($quotedColumns) > 0) {
            $select = implode(', ', $quotedColumns);
        }

        return sprintf('SELECT %s FROM %s', $select, $this->getQuotedTableWithScheme());
    }

    /**
     * @return string[]
     */
    public function getColumnsNames(): array
    {
        return $this->columnsNames;
    }

    public function getQuotedTableWithScheme(): string
    {
        return sprintf(
            '%s.%s',
            $this->platform->quoteSingleIdentifier($this->schema),
            $this->platform->quoteSingleIdentifier($this->tableName)
        );
    }

    public function getPrimaryKeysNames(): ?array
    {
        return $this->primaryKeysNames;
    }

    public function getQueryBindings(): array
    {
        return [];
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
