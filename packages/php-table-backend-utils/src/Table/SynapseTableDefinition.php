<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table;

use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;
use Keboola\TableBackendUtils\Table\Synapse\TableDistributionDefinition;
use Keboola\TableBackendUtils\Table\Synapse\TableIndexDefinition;

final class SynapseTableDefinition implements TableDefinitionInterface
{
    /** @var string */
    private $schemaName;

    /** @var string */
    private $tableName;

    /** @var ColumnCollection */
    private $columns;

    /** @var string[] */
    private $primaryKeysNames;

    /** @var TableDistributionDefinition */
    private $tableDistribution;

    /** @var bool */
    private $isTemporary;

    /** @var TableIndexDefinition */
    private $tableIndex;

    /**
     * @param string[] $primaryKeysNames
     */
    public function __construct(
        string $schemaName,
        string $tableName,
        bool $isTemporary,
        ColumnCollection $columns,
        array $primaryKeysNames,
        TableDistributionDefinition $tableDistribution,
        TableIndexDefinition $tableIndex
    ) {
        $this->schemaName = $schemaName;
        $this->tableName = $tableName;
        $this->columns = $columns;
        $this->primaryKeysNames = $primaryKeysNames;
        $this->tableDistribution = $tableDistribution;
        $this->isTemporary = $isTemporary;
        $this->tableIndex = $tableIndex;
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

    public function getTableDistribution(): TableDistributionDefinition
    {
        return $this->tableDistribution;
    }

    public function getTableIndex(): TableIndexDefinition
    {
        return $this->tableIndex;
    }
}
