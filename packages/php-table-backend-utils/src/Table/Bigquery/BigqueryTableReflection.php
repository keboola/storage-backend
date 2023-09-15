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
use Throwable;

class BigqueryTableReflection implements TableReflectionInterface
{
    public const DEPENDENT_OBJECT_TABLE = 'TABLE';
    public const DEPENDENT_OBJECT_VIEW = 'VIEW';

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
        if ($this->exists() === false) {
            throw new TableNotExistsReflectionException(sprintf('Table "%s" not found.', $this->tableName));
        }
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
        if ($this->exists() === false) {
            throw new TableNotExistsReflectionException(sprintf('Table "%s" not found.', $this->tableName));
        }
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
        if ($this->exists() === false) {
            throw new TableNotExistsReflectionException(sprintf('Table "%s" not found.', $this->tableName));
        }
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
        return $this->bqClient->dataset($this->datasetName)->table($this->tableName)->exists();
    }

    public static function getDependentViewsForObject(
        BigQueryClient $bqClient,
        string $objectName,
        string $schemaName,
        string $objectType = self::DEPENDENT_OBJECT_TABLE
    ): array {
        $views = $bqClient->runQuery(
            $bqClient->query(
                sprintf(
                    'SELECT * FROM %s.INFORMATION_SCHEMA.VIEWS;',
                    BigqueryQuote::quoteSingleIdentifier($schemaName)
                )
            )
        );

        $objectNameWithSchema = sprintf(
            '%s.%s',
            BigqueryQuote::quoteSingleIdentifier($schemaName),
            BigqueryQuote::quoteSingleIdentifier($objectName)
        );
        $dependentViews = [];
        foreach ($views as $viewRow) {
            if ($viewRow['view_definition'] === null
                || strpos($viewRow['view_definition'], $objectNameWithSchema) === false
            ) {
                continue;
            }

            $dependentViews[] = [
                'schema_name' => $viewRow['table_schema'],
                'name' => $viewRow['table_name'],
            ];
        }

        return $dependentViews;
    }
}
