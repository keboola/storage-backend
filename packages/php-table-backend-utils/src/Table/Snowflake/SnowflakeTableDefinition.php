<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Snowflake;

use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

final class SnowflakeTableDefinition implements TableDefinitionInterface
{
    /** @var string */
    private $schemaName;

    /** @var string */
    private $tableName;

    /** @var ColumnCollection */
    private $columns;

    /** @var string[] */
    private $primaryKeysNames;

    /** @var bool */
    private $isTemporary;

    /**
     * @param string[] $primaryKeysNames
     */
    public function __construct(
        string $schemaName,
        string $tableName,
        bool $isTemporary,
        ColumnCollection $columns,
        array $primaryKeysNames
    ) {
        $this->schemaName = $schemaName;
        $this->tableName = $tableName;
        $this->columns = $columns;
        $this->primaryKeysNames = $primaryKeysNames;
        $this->isTemporary = $isTemporary;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }

    public function getSchemaName(): string
    {
        return $this->schemaName;
    }

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

    public function getPrimaryKeysNames(): array
    {
        return $this->primaryKeysNames;
    }

    public function isTemporary(): bool
    {
        return $this->isTemporary;
    }
}
