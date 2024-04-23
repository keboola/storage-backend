<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\ToStage;

use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\Db\ImportExport\Backend\Assert;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Exception\ColumnsMismatchException;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\Bigquery\SelectSource;
use Keboola\Db\ImportExport\Storage\Bigquery\Table;
use Keboola\TableBackendUtils\Connection\Bigquery\QueryExecutor;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

class FromTableInsertIntoAdapter implements CopyAdapterInterface
{
    private BigQueryClient $bqClient;

    public function __construct(BigQueryClient $bqClient)
    {
        $this->bqClient = $bqClient;
    }

    /**
     * @throws ColumnsMismatchException
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        TableDefinitionInterface $destination,
        ImportOptionsInterface $importOptions,
    ): int {
        assert($source instanceof SelectSource || $source instanceof Table);
        assert($destination instanceof BigqueryTableDefinition);
        assert($importOptions instanceof BigqueryImportOptions);

        $quotedColumns = array_map(static function ($column) {
            return BigqueryQuote::quoteSingleIdentifier($column);
        }, $destination->getColumnsNames());

        if ($source instanceof Table && $importOptions->usingUserDefinedTypes()) {
            Assert::assertSameColumns(
                (new BigqueryTableReflection(
                    $this->bqClient,
                    $source->getSchema(),
                    $source->getTableName(),
                ))->getColumnsDefinitions(),
                $destination->getColumnsDefinitions(),
            );
        }

        $select = $source->getFromStatement();
        if (!$importOptions->usingUserDefinedTypes()) {
            // if destination table is string (SAME_TABLES_NOT_REQUIRED) cast values to string
            $select = $source->getFromStatementWithStringCasting();
        }

        $sql = sprintf(
            'INSERT INTO %s.%s (%s) %s',
            BigqueryQuote::quoteSingleIdentifier($destination->getSchemaName()),
            BigqueryQuote::quoteSingleIdentifier($destination->getTableName()),
            implode(', ', $quotedColumns),
            $select,
        );

        if ($source instanceof SelectSource) {
            (new QueryExecutor($this->bqClient))->runQuery($this->bqClient->query($sql)->parameters(
                $source->getQueryBindings(),
            ));
        } else {
            (new QueryExecutor($this->bqClient))->runQuery($this->bqClient->query($sql));
        }

        $ref = new BigqueryTableReflection(
            $this->bqClient,
            $destination->getSchemaName(),
            $destination->getTableName(),
        );

        return $ref->getRowsCount();
    }
}
