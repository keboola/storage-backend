<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Bigquery;

use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\TableType;

final class BigqueryTableDefinition implements TableDefinitionInterface
{
    private string $schemaName;

    private string $tableName;

    private ColumnCollection $columns;

    private bool $isTemporary;

    /** @var string[] */
    private array $primaryKeysNames;

    /**
     * @param string[] $primaryKeysNames
     */
    public function __construct(
        string $schemaName,
        string $tableName,
        bool $isTemporary,
        ColumnCollection $columns,
        array $primaryKeysNames,
    ) {
        $this->schemaName = $schemaName;
        $this->tableName = $tableName;
        $this->columns = $columns;
        $this->isTemporary = $isTemporary;
        $this->primaryKeysNames = $primaryKeysNames;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }

    //@todo nema to ist do interface ?
    public function getSchemaName(): string
    {
        return $this->schemaName;
    }

    /**
     * @return string[]
     */
    public function getColumnsNames(): array
    {
        $names = [];
        /** @var ColumnInterface $column */
        foreach ($this->columns as $column) {
            $names[] = $column->getColumnName();
        }

        return $names;
    }

    public function getColumnsDefinitions(): ColumnCollection
    {
        return $this->columns;
    }

    /**
     * @return string[]
     */
    public function getPrimaryKeysNames(): array
    {
        return $this->primaryKeysNames;
    }

    public function isTemporary(): bool
    {
        return $this->isTemporary;
    }

    public function getTableType(): TableType
    {
        return TableType::TABLE;
    }
}
