<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Exception\JobException;
use Google\Cloud\Core\Exception\ServiceException;
use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryException;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\Backend\Helper\BackendHelper;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\ToFinalTableImporterInterface;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\TableBackendUtils\Connection\Bigquery\Session;
use Keboola\TableBackendUtils\Connection\Bigquery\SessionFactory;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

final class FullImporter implements ToFinalTableImporterInterface
{
    private const TIMER_COPY_TO_TARGET = 'copyFromStagingToTarget';
    private const TIMER_DEDUP = 'fromStagingToTargetWithDedup';

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
        BigqueryImportOptions $options,
        ImportState $state,
        Session $session
    ): void {
        // truncate destination table
        $this->bqClient->runQuery($this->bqClient->query(
            $this->sqlBuilder->getTruncateTable(
                $destinationTableDefinition->getSchemaName(),
                $destinationTableDefinition->getTableName()
            ),
            $session->getAsQueryOptions()
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
            $sql,
            $session->getAsQueryOptions()
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
        assert($options instanceof BigqueryImportOptions);

        $session = $options->getSession();
        if ($session === null) {
            $session = (new SessionFactory($this->bqClient))->createSession();
        }

        /** @var BigqueryTableDefinition $destinationTableDefinition */
        try {
            //import files to staging table
            if (!empty($destinationTableDefinition->getPrimaryKeysNames())) {
                $this->doFullLoadWithDedup(
                    $stagingTableDefinition,
                    $destinationTableDefinition,
                    $options,
                    $state,
                    $session
                );
            } else {
                $this->doLoadFullWithoutDedup(
                    $stagingTableDefinition,
                    $destinationTableDefinition,
                    $options,
                    $state,
                    $session
                );
            }
        } catch (JobException|ServiceException $e) {
            throw BigqueryException::covertException($e);
        }

        $state->setImportedColumns($stagingTableDefinition->getColumnsNames());

        return $state->getResult();
    }


    private function doFullLoadWithDedup(
        BigqueryTableDefinition $stagingTableDefinition,
        BigqueryTableDefinition $destinationTableDefinition,
        BigqueryImportOptions $options,
        ImportState $state,
        Session $session
    ): void {
        $state->startTimer(self::TIMER_DEDUP);

        // 1. Create table for deduplication
        $deduplicationTableName = BackendHelper::generateTempDedupTableName();

        try {
            // 2 transfer data from source to dedup table with dedup process
            $this->bqClient->runQuery($this->bqClient->query(
                $this->sqlBuilder->getCreateDedupTable(
                    $stagingTableDefinition,
                    $deduplicationTableName,
                    $destinationTableDefinition->getPrimaryKeysNames()
                ),
                $session->getAsQueryOptions()
            ));
            /** @var BigqueryTableDefinition $deduplicationTableDefinition */
            $deduplicationTableDefinition = (new BigqueryTableReflection(
                $this->bqClient,
                $stagingTableDefinition->getSchemaName(),
                $deduplicationTableName
            ))->getTableDefinition();

            // 3 truncate destination table
            $this->bqClient->runQuery($this->bqClient->query(
                $this->sqlBuilder->getTruncateTable(
                    $destinationTableDefinition->getSchemaName(),
                    $destinationTableDefinition->getTableName()
                ),
                $session->getAsQueryOptions()
            ));

            $this->bqClient->runQuery($this->bqClient->query(
                $this->sqlBuilder->getBeginTransaction(),
                $session->getAsQueryOptions()
            ));

            // 4 move data with INSERT INTO
            $this->bqClient->runQuery($this->bqClient->query(
                $this->sqlBuilder->getInsertAllIntoTargetTableCommand(
                    $deduplicationTableDefinition,
                    $destinationTableDefinition,
                    $options,
                    DateTimeHelper::getNowFormatted()
                ),
                $session->getAsQueryOptions()
            ));
            $state->stopTimer(self::TIMER_DEDUP);

            $this->bqClient->runQuery($this->bqClient->query(
                $this->sqlBuilder->getCommitTransaction(),
                $session->getAsQueryOptions()
            ));
        } finally {
            if (isset($deduplicationTableDefinition)) {
                // 5 drop dedup table
                $this->bqClient->runQuery($this->bqClient->query(
                    $this->sqlBuilder->getDropTableIfExistsCommand(
                        $deduplicationTableDefinition->getSchemaName(),
                        $deduplicationTableDefinition->getTableName()
                    ),
                    $session->getAsQueryOptions()
                ));
            }
        }
    }
}
