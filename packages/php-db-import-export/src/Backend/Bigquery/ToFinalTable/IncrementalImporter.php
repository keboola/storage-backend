<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable;

use Exception as InternalException;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Exception\JobException;
use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryException;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\ToFinalTableImporterInterface;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\TableBackendUtils\Connection\Bigquery\SessionFactory;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

final class IncrementalImporter implements ToFinalTableImporterInterface
{
    private const TIMER_INSERT_INTO_TARGET = 'insertIntoTargetFromStaging';

    private BigQueryClient $bqClient;

    private SqlBuilder $sqlBuilder;

    public function __construct(BigQueryClient $bqClient)
    {
        $this->bqClient = $bqClient;
        $this->sqlBuilder = new SqlBuilder();
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
        // table used in getInsertAllIntoTargetTableCommand if PK's are specified, dedup table is used
        $tableToCopyFrom = $stagingTableDefinition;

        $timestampValue = DateTimeHelper::getNowFormatted();
        try {
            $this->bqClient->runQuery($this->bqClient->query(
                $this->sqlBuilder->getBeginTransaction(),
                $session->getAsQueryOptions()
            ));

            if (!empty($destinationTableDefinition->getPrimaryKeysNames())) {
                $deduplicationTableDefinition = null;
                // dedup
                throw new InternalException('not implemented yet');
            }

            // insert into destination table
            $state->startTimer(self::TIMER_INSERT_INTO_TARGET);
            $this->bqClient->runQuery($this->bqClient->query(
                $this->sqlBuilder->getInsertAllIntoTargetTableCommand(
                    $tableToCopyFrom,
                    $destinationTableDefinition,
                    $options,
                    $timestampValue
                ),
                $session->getAsQueryOptions()
            ));
            $state->stopTimer(self::TIMER_INSERT_INTO_TARGET);

            $this->bqClient->runQuery($this->bqClient->query(
                $this->sqlBuilder->getCommitTransaction(),
                $session->getAsQueryOptions()
            ));

            $state->setImportedColumns($stagingTableDefinition->getColumnsNames());
        } catch (JobException $e) {
            throw BigqueryException::covertException($e);
        } finally {
            if (isset($deduplicationTableDefinition)) {
                // drop dedup table
                $this->bqClient->runQuery($this->bqClient->query(
                    $this->sqlBuilder->getDropTableIfExistsCommand(
                        $deduplicationTableDefinition->getSchemaName(),
                        $deduplicationTableDefinition->getTableName()
                    ),
                    $session->getAsQueryOptions()
                ));
            }
        }

        return $state->getResult();
    }
}
