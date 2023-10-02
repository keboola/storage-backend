<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Bigquery;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Table;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\TableReflectionInterface;
use Keboola\TableBackendUtils\Table\TableStats;
use Keboola\TableBackendUtils\Table\TableStatsInterface;
use Keboola\TableBackendUtils\TableNotExistsReflectionException;
use LogicException;

/**
 * @phpstan-import-type BigqueryTableFieldSchema from Bigquery
 */
class BigqueryTableReflection implements TableReflectionInterface
{
    private string $datasetName;

    private Table $table;

    private string $tableName;

    private bool $isTemporary = false;

    public function __construct(BigQueryClient $bqClient, string $datasetName, string $tableName)
    {
        $this->tableName = $tableName;
        $this->datasetName = $datasetName;
        $this->table = $bqClient->dataset($this->datasetName)->table($this->tableName);
    }

    private function throwIfNotExists(): void
    {
        if ($this->exists() === false) {
            throw new TableNotExistsReflectionException(sprintf('Table "%s" not found.', $this->tableName));
        }
    }

    /** @return  string[] */
    public function getColumnsNames(): array
    {
        $this->throwIfNotExists();

        $columns = [];
        /**
         * @phpstan-var BigqueryTableFieldSchema $row
         */
        foreach ($this->table->info()['schema']['fields'] as $row) {
            $columns[] = $row['name'];
        }
        return $columns;
    }

    public function getColumnsDefinitions(): ColumnCollection
    {
        $this->throwIfNotExists();
        $columns = [];
        /**
         * @phpstan-var BigqueryTableFieldSchema $row
         */
        foreach ($this->table->info()['schema']['fields'] as $row) {
            $columns[] = BigqueryColumn::createFromDB($row);
        }

        return new ColumnCollection($columns);
    }

    public function getRowsCount(): int
    {
        $this->throwIfNotExists();
        return (int) $this->table->info()['numRows'];
    }

    /** @return  array<string> */
    public function getPrimaryKeysNames(): array
    {
        return [];
    }

    public function getTableStats(): TableStatsInterface
    {
        $this->throwIfNotExists();
        return new TableStats((int) $this->table->info()['numBytes'], $this->getRowsCount());
    }

    public function isTemporary(): bool
    {
        return $this->isTemporary;
    }

    /**
     * @return array<int, array<string, mixed>>
     * array{
     *  schema_name: string,
     *  name: string
     * }[]
     */
    public function getDependentViews(): array
    {
        throw new LogicException('Not implemented');
    }

    public function getTableDefinition(): TableDefinitionInterface
    {
        return new BigqueryTableDefinition(
            $this->datasetName,
            $this->tableName,
            $this->isTemporary(),
            $this->getColumnsDefinitions(),
            $this->getPrimaryKeysNames()
        );
    }

    public function exists(): bool
    {
        return $this->table->exists();
    }

    public function refresh(): void
    {
        $this->table->reload();
    }
}
