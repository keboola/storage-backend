<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Keboola\Db\Import\Exception;
use Keboola\Db\Import\Result;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\BackendHelper;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\SourceStorage;

class Importer implements ImporterInterface
{
    public const TIMESTAMP_COLUMN_NAME = '_timestamp';

    /** @var Connection */
    private $connection;

    /**
     * @var SqlCommandBuilder
     */
    private $sqlBuilder;

    /**
     * @var ImportState
     */
    private $importState;

    public function __construct(
        Connection $connection
    ) {
        $this->connection = $connection;
        $this->sqlBuilder = new SqlCommandBuilder();
    }

    public function importTable(
        ImportOptions $options,
        SourceStorage\SourceInterface $source
    ): Result {
        $this->importState = new ImportState(BackendHelper::generateStagingTableName());
        $this->validateColumns($options);

        $this->createTable($options, $this->importState->getStagingTableName());

        try {
            //import files to staging table
            $this->importToStagingTable($options, $source);
            $primaryKeys = $this->connection->getTablePrimaryKey(
                $options->getSchema(),
                $options->getTableName()
            );
            if ($options->isIncremental()) {
                $this->doIncrementalLoad($options, $primaryKeys);
            } else {
                $this->doNonIncrementalLoad($options, $primaryKeys);
            }
            $this->importState->setImportedColumns($options->getColumns());
        } finally {
            $this->runQuery(
                $this->sqlBuilder->getDropCommand($options->getSchema(), $this->importState->getStagingTableName())
            );
        }

        return $this->importState->getResult();
    }

    private function validateColumns(ImportOptions $importOptions): void
    {
        if (count($importOptions->getColumns()) === 0) {
            throw new Exception(
                'No columns found in CSV file.',
                Exception::NO_COLUMNS
            );
        }

        $tableColumns = $this->connection->getTableColumns(
            $importOptions->getSchema(),
            $importOptions->getTableName()
        );

        $moreColumns = array_diff($importOptions->getColumns(), $tableColumns);
        if (!empty($moreColumns)) {
            throw new Exception(
                'Columns doest not match. Non existing columns: ' . implode(', ', $moreColumns),
                Exception::COLUMNS_COUNT_NOT_MATCH
            );
        }
    }

    private function createTable(
        ImportOptions $importOptions,
        string $tableName
    ): void {
        $this->runQuery($this->sqlBuilder->getCreateStagingTableCommand(
            $importOptions->getSchema(),
            $tableName,
            $importOptions->getColumns()
        ));
    }

    private function runQuery(string $query, ?string $timerName = null): void
    {
        if ($timerName) {
            $this->importState->startTimer($timerName);
        }
        // echo sprintf("Executing query: %s \n", $query);

        $this->connection->query($query);
        if ($timerName) {
            $this->importState->stopTimer($timerName);
        }
    }

    private function importToStagingTable(
        ImportOptions $importOptions,
        SourceStorage\SourceInterface $source
    ): void {
        $adapter = $source->getBackendImportAdapter($this);
        if (!$adapter instanceof SnowflakeImportAdapterInterface) {
            throw new \Exception(sprintf(
                'Adapter "%s" must implement "SnowflakeImportAdapterInterface".',
                get_class($adapter)
            ));
        }
        $commands = $adapter->getCopyCommands($importOptions, $this->importState->getStagingTableName());
        $this->importState->addImportedRowsCount(
            $adapter->executeCopyCommands($commands, $this->connection, $importOptions, $this->importState)
        );
    }

    private function doIncrementalLoad(
        ImportOptions $importOptions,
        array $primaryKeys
    ): void {
        $this->runQuery(
            $this->sqlBuilder->getBeginTransaction()
        );
        if (!empty($primaryKeys)) {
            $this->runQuery(
                $this->sqlBuilder->getUpdateWithPkCommand(
                    $importOptions,
                    $this->importState->getStagingTableName(),
                    $primaryKeys
                ),
                'updateTargetTable'
            );
            $this->runQuery(
                $this->sqlBuilder->getDeleteOldItemsCommand(
                    $importOptions,
                    $this->importState->getStagingTableName(),
                    $primaryKeys
                ),
                'deleteUpdatedRowsFromStaging'
            );
            $this->importState->startTimer('dedupStaging');
            $this->dedup($importOptions, $primaryKeys);
            $this->importState->stopTimer('dedupStaging');
        }
        $this->runQuery(
            $this->sqlBuilder->getInsertFromStagingToTargetTableCommand(
                $importOptions,
                $this->importState->getStagingTableName()
            ),
            'insertIntoTargetFromStaging'
        );
        $this->runQuery(
            $this->sqlBuilder->getCommitTransaction()
        );
    }

    /**
     * @param ImportOptions $importOptions
     * @param array $primaryKeys
     */
    private function dedup(
        ImportOptions $importOptions,
        array $primaryKeys
    ): void {
        $tempTableName = BackendHelper::generateStagingTableName();
        $this->createTable($importOptions, $tempTableName);
        $this->runQuery(
            $this->sqlBuilder->getDedupCommand(
                $importOptions,
                $primaryKeys,
                $this->importState->getStagingTableName(),
                $tempTableName
            )
        );
        $this->runQuery(
            $this->sqlBuilder->getDropCommand(
                $importOptions->getSchema(),
                $this->importState->getStagingTableName()
            )
        );
        $this->runQuery(
            $this->sqlBuilder->getRenameTableCommand(
                $importOptions->getSchema(),
                $tempTableName,
                $this->importState->getStagingTableName()
            )
        );
    }

    private function doNonIncrementalLoad(
        ImportOptions $importOptions,
        array $primaryKeys
    ): void {
        if (!empty($primaryKeys)) {
            $this->importState->startTimer('dedup');
            $this->dedup($importOptions, $primaryKeys);
            $this->importState->stopTimer('dedup');
        }
        $this->runQuery(
            $this->sqlBuilder->getBeginTransaction()
        );
        $this->runQuery(
            $this->sqlBuilder->getTruncateTableCommand(
                $importOptions->getSchema(),
                $importOptions->getTableName()
            )
        );
        $this->runQuery(
            $this->sqlBuilder->getInsertAllIntoTargetTableCommand(
                $importOptions,
                $this->importState->getStagingTableName()
            ),
            'copyFromStagingToTarget'
        );
        $this->runQuery(
            $this->sqlBuilder->getCommitTransaction()
        );
    }
}
