<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable;

use Exception as InternalException;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Exception\JobException;
use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryException;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\ToFinalTableImporterInterface;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

final class FullImporter implements ToFinalTableImporterInterface
{
    private const TIMER_COPY_TO_TARGET = 'copyFromStagingToTarget';

    private BigQueryClient $bqClient;

    private SqlBuilder $sqlBuilder;

    public function __construct(BigQueryClient $bqClient)
    {
        $this->bqClient = $bqClient;
        $this->sqlBuilder = new SqlBuilder();
    }

    private function doLoadFullWithoutDedup(
        BigqueryTableDefinition $stagingTableDefinition,
        BigqueryTableDefinition $destinationTableDefinition,
        ImportOptions $options,
        ImportState $state
    ): void {
        // truncate destination table
        $this->bqClient->runQuery($this->bqClient->query(
            $this->sqlBuilder->getTruncateTable(
                $destinationTableDefinition->getSchemaName(),
                $destinationTableDefinition->getTableName()
            )
        ));
        $state->startTimer(self::TIMER_COPY_TO_TARGET);

        // move data with INSERT INTO
        $sql = $this->sqlBuilder->getInsertAllIntoTargetTableCommand(
            $stagingTableDefinition,
            $destinationTableDefinition,
            $options,
            DateTimeHelper::getNowFormatted()
        );
        $this->bqClient->runQuery($this->bqClient->query(
            $sql
        ));
        $state->stopTimer(self::TIMER_COPY_TO_TARGET);
    }

    public function importToTable(
        TableDefinitionInterface $stagingTableDefinition,
        TableDefinitionInterface $destinationTableDefinition,
        ImportOptionsInterface $options,
        ImportState $state
    ): Result {
        assert($stagingTableDefinition instanceof BigqueryTableDefinition);
        assert($destinationTableDefinition instanceof BigqueryTableDefinition);
        assert($options instanceof ImportOptions);

        /** @var BigqueryTableDefinition $destinationTableDefinition */
        try {
            //import files to staging table
            if (!empty($destinationTableDefinition->getPrimaryKeysNames())) {
                // dedup
                throw new InternalException('not implemented yet');
            } else {
                $this->doLoadFullWithoutDedup(
                    $stagingTableDefinition,
                    $destinationTableDefinition,
                    $options,
                    $state
                );
            }
        } catch (JobException $e) {
            throw BigqueryException::covertException($e);
        }

        $state->setImportedColumns($stagingTableDefinition->getColumnsNames());

        return $state->getResult();
    }
}
