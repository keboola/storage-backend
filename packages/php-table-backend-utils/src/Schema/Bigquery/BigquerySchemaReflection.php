<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Schema\Bigquery;

use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\ReflectionException;
use Keboola\TableBackendUtils\Schema\SchemaReflectionInterface;
use Keboola\TableBackendUtils\TableNotExistsReflectionException;
use LogicException;

class BigquerySchemaReflection implements SchemaReflectionInterface
{
    private BigQueryClient $bqClient;

    private string $datasetName;

    public function __construct(BigQueryClient $bqClient, string $datasetName)
    {
        $this->datasetName = $datasetName;
        $this->bqClient = $bqClient;
    }

    /**
     * @return string[]
     */
    public function getTablesNames(): array
    {
        $isDatasetExist = $this->bqClient->dataset($this->datasetName)->exists();
        if ($isDatasetExist === false) {
            throw new ReflectionException(sprintf('Dataset "%s" not found.', $this->datasetName));
        }
        $query = $this->bqClient->query(
            sprintf(
                'SELECT * FROM %s.INFORMATION_SCHEMA.TABLES WHERE `table_type` != \'VIEW\';',
                BigqueryQuote::quoteSingleIdentifier($this->datasetName),
            ),
        );
        $queryResults = $this->bqClient->runQuery($query);

        $tables = [];
        /**
         * @var array{
         *  table_catalog: string,
         *  table_schema: string,
         *  table_name: string,
         *  table_type: string,
         *  is_insertable_into: string,
         *  is_typed: string,
         *  creation_time: string,
         *  base_table_catalog: string,
         *  base_table_schema: string,
         *  base_table_name: string,
         *  snapshot_time_ms: string,
         *  ddl: string,
         *  snapshot_time_ms: string,
         *  default_collation_name: string,
         *  upsert_stream_apply_watermark: string,
         * } $row
         */
        foreach ($queryResults as $row) {
            $tables[] = $row['table_name'];
        }
        return $tables;
    }

    /**
     * @return string[]
     */
    public function getViewsNames(): array
    {
        $isDatasetExist = $this->bqClient->dataset($this->datasetName)->exists();
        if ($isDatasetExist === false) {
            throw new ReflectionException(sprintf('Dataset "%s" not found', $this->datasetName));
        }
        $query = $this->bqClient->query(
            sprintf(
                'SELECT * FROM %s.INFORMATION_SCHEMA.VIEWS;',
                BigqueryQuote::quoteSingleIdentifier($this->datasetName),
            ),
        );
        $queryResults = $this->bqClient->runQuery($query);

        $tables = [];
        /**
         * @var array{
         *  table_catalog: string,
         *  table_schema: string,
         *  table_name: string,
         *  table_type: string,
         *  is_insertable_into: string,
         *  is_typed: string,
         *  creation_time: string,
         *  base_table_catalog: string,
         *  base_table_schema: string,
         *  base_table_name: string,
         *  snapshot_time_ms: string,
         *  ddl: string,
         *  snapshot_time_ms: string,
         *  default_collation_name: string,
         *  upsert_stream_apply_watermark: string,
         * } $row
         */
        foreach ($queryResults as $row) {
            $tables[] = $row['table_name'];
        }
        return $tables;
    }
}
