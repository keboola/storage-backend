<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Bigquery;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Table;
use Google\Cloud\BigQuery\Timestamp;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
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
    private Table $table;

    private bool $isTemporary = false;

    public function __construct(
        private readonly BigQueryClient $bqClient,
        private readonly string $datasetName,
        private readonly string $tableName
    ) {
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

    /**
     * @return Partition[]
     */
    public function getPartitionsList(): array
    {
        $this->throwIfNotExists();
        $query = $this->bqClient->query(sprintf(
            'SELECT * FROM %s.INFORMATION_SCHEMA.PARTITIONS WHERE table_name = %s',
            BigqueryQuote::quoteSingleIdentifier($this->datasetName),
            BigqueryQuote::quote($this->tableName)
        ));

        $result = $this->bqClient->runQuery($query);

        $partitions = [];
        /**
         * @var array{
         *      partition_id: string,
         *      total_rows: int,
         *      last_modified_time: Timestamp,
         *      storage_tier: string
         *  } $partition
         */
        foreach ($result as $partition) {
            $partitions[] = new Partition(
                $partition['partition_id'],
                (string) $partition['total_rows'],
                (string) $partition['last_modified_time'],
                $partition['storage_tier'],
            );
        }

        return $partitions;
    }
}
