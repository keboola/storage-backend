<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Bigquery;

use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\TableReflectionInterface;
use Keboola\TableBackendUtils\Table\TableStats;
use Keboola\TableBackendUtils\Table\TableStatsInterface;
use Keboola\TableBackendUtils\TableNotExistsReflectionException;
use LogicException;

class BigqueryTableReflection implements TableReflectionInterface
{
    private BigQueryClient $bqClient;

    private string $datasetName;

    private string $tableName;

    private bool $isTemporary = false;

    public function __construct(BigQueryClient $bqClient, string $datasetName, string $tableName)
    {
        $this->tableName = $tableName;
        $this->datasetName = $datasetName;
        $this->bqClient = $bqClient;
    }

    /** @return  string[] */
    public function getColumnsNames(): array
    {
        $query = $this->bqClient->query(
            sprintf(
                'SELECT column_name FROM %s.INFORMATION_SCHEMA.COLUMNS WHERE table_name = %s',
                BigqueryQuote::quoteSingleIdentifier($this->datasetName),
                BigqueryQuote::quote($this->tableName)
            )
        );
        $queryResults = $this->bqClient->runQuery($query);

        $columns = [];
        /**
         * @var array{
         *  table_catalog: string,
         *  table_schema: string,
         *  table_name: string,
         *  column_name: string,
         *  ordinal_position: int,
         *  is_nullable: string,
         *  data_type: string,
         *  is_hidden: string,
         *  is_system_defined: string,
         *  is_partitioning_column: string,
         *  clustering_ordinal_position: ?string,
         *  collation_name: string,
         *  column_default: string,
         *  rounding_mode: ?string,
         * } $row
         */
        foreach ($queryResults as $row) {
            $columns[] = $row['column_name'];
        }
        return $columns;
    }

    public function getColumnsDefinitions(): ColumnCollection
    {
        $query = $this->bqClient->query(
            sprintf(
                'SELECT * EXCEPT(is_generated, generation_expression, is_stored, is_updatable) 
FROM %s.INFORMATION_SCHEMA.COLUMNS WHERE table_name = %s',
                BigqueryQuote::quoteSingleIdentifier($this->datasetName),
                BigqueryQuote::quote($this->tableName)
            )
        );

        $queryResults = $this->bqClient->runQuery($query);

        $columns = [];
        /**
         * @var array{
         *  table_catalog: string,
         *  table_schema: string,
         *  table_name: string,
         *  column_name: string,
         *  ordinal_position: int,
         *  is_nullable: string,
         *  data_type: string,
         *  is_hidden: string,
         *  is_system_defined: string,
         *  is_partitioning_column: string,
         *  clustering_ordinal_position: ?string,
         *  collation_name: string,
         *  column_default: string,
         *  rounding_mode: ?string,
         * } $row
         */
        foreach ($queryResults as $row) {
            $columns[] = BigqueryColumn::createFromDB($row);
        }

        return new ColumnCollection($columns);
    }

    public function getRowsCount(): int
    {
        $query = $this->bqClient->query(sprintf(
            'SELECT COUNT(*) AS NumberOfRows FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($this->datasetName),
            BigqueryQuote::quoteSingleIdentifier($this->tableName)
        ));

        $result = $this->bqClient->runQuery($query);

        /** @var array<string, string> $current */
        $current = $result->getIterator()->current();
        return (int) $current['NumberOfRows'];
    }

    /** @return  array<string> */
    public function getPrimaryKeysNames(): array
    {
        return [];
    }

    public function getTableStats(): TableStatsInterface
    {
        $sql = sprintf(
            'SELECT size_bytes FROM %s.__TABLES__ WHERE table_id=%s',
            BigqueryQuote::quoteSingleIdentifier($this->datasetName),
            BigqueryQuote::quote($this->tableName)
        );
        $result = $this->bqClient->runQuery($this->bqClient->query($sql));

        /** @var array<string, string>|null $current */
        $current = $result->getIterator()->current();
        if ($current === null) {
            throw new TableNotExistsReflectionException('Table does not exist');
        }

        return new TableStats((int) $current['size_bytes'], $this->getRowsCount());
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
        throw new LogicException('Not implemented');
    }
}
