<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Bigquery;

use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

final class BigqueryTableDefinition implements TableDefinitionInterface
{
    private string $tableName;

    private ColumnCollection $columns;

    private bool $isTemporary;

    public function __construct(
        string $tableName,
        bool $isTemporary,
        ColumnCollection $columns
    ) {
        $this->tableName = $tableName;
        $this->columns = $columns;
        $this->isTemporary = $isTemporary;
    }

    public function getTableName(): string
    {
        return $this->tableName;
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
        return []; //bigquery doesn't support primary keys
    }

    public function isTemporary(): bool
    {
        return $this->isTemporary;
    }
}
