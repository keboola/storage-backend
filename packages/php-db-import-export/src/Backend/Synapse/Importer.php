<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse;

use Doctrine\DBAL\Connection;
use Keboola\Db\Import\Exception;
use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Synapse\Helper\BackendHelper;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;

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
        $this->sqlBuilder = new SqlCommandBuilder($this->connection);
    }

    public function importTable(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptions $options
    ): Result {
        if (!$destination instanceof Storage\Synapse\Table) {
            throw new \Exception(sprintf(
                'Destination "%s" is invalid only "%s" is supported.',
                get_class($destination),
                Storage\Synapse\Table::class
            ));
        }
        if ($source instanceof Storage\ABS\SourceFile
            && $source->getCsvOptions()->getEnclosure() === ''
        ) {
            throw new \Exception(
                'CSV property FIELDQUOTE|ECLOSURE must be set when using Synapse analytics.'
            );
        }

        $this->importState = new ImportState(BackendHelper::generateTempTableName());
        $this->validateColumns($options, $destination);

        $this->runQuery($this->sqlBuilder->getCreateTempTableCommand(
            $destination->getSchema(),
            $this->importState->getStagingTableName(),
            $options->getColumns()
        ));

        try {
            //import files to staging table
            $this->importToStagingTable($source, $destination, $options);
            $primaryKeys = $this->sqlBuilder->getTablePrimaryKey(
                $destination->getSchema(),
                $destination->getTableName()
            );
            if ($options->isIncremental()) {
                $this->doIncrementalLoad($options, $destination, $primaryKeys);
            } else {
                $this->doNonIncrementalLoad($options, $destination, $primaryKeys);
            }
            $this->importState->setImportedColumns($options->getColumns());
        } finally {
            $this->runQuery(
                $this->sqlBuilder->getDropCommand($destination->getSchema(), $this->importState->getStagingTableName())
            );
        }

        return $this->importState->getResult();
    }

    private function validateColumns(
        ImportOptions $importOptions,
        Storage\Synapse\Table $destination
    ): void {
        if (count($importOptions->getColumns()) === 0) {
            throw new Exception(
                'No columns found in CSV file.',
                Exception::NO_COLUMNS
            );
        }

        $tableColumns = $this->sqlBuilder->getTableColumns(
            $destination->getSchema(),
            $destination->getTableName()
        );

        $moreColumns = array_diff($importOptions->getColumns(), $tableColumns);
        if (!empty($moreColumns)) {
            throw new Exception(
                'Columns doest not match. Non existing columns: ' . implode(', ', $moreColumns),
                Exception::COLUMNS_COUNT_NOT_MATCH
            );
        }
    }

    private function runQuery(string $query, ?string $timerName = null): void
    {
        if ($timerName) {
            $this->importState->startTimer($timerName);
        }
        // echo sprintf("Executing query: %s \n", $query);
        $this->connection->exec($query);
        if ($timerName) {
            $this->importState->stopTimer($timerName);
        }
    }

    /**
     * @param Storage\Synapse\Table $destination
     */
    private function importToStagingTable(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptions $importOptions
    ): void {
        $adapter = $source->getBackendImportAdapter($this);
        $commands = $adapter->getCopyCommands(
            $destination,
            $importOptions,
            $this->importState->getStagingTableName(),
            $this->connection
        );

        try {
            $this->importState->startTimer('copyToStaging');
            foreach ($commands as $command) {
                $this->runQuery($command);
            }
            $this->importState->stopTimer('copyToStaging');
        } catch (\Throwable $e) {
            $stringCode = Exception::INVALID_SOURCE_DATA;
            if (strpos($e->getMessage(), 'was not found') !== false) {
                $stringCode = Exception::MANDATORY_FILE_NOT_FOUND;
            }
            throw new Exception('Load error: ' . $e->getMessage(), $stringCode, $e);
        }

        $rows = $this->connection->fetchAll($this->sqlBuilder->getTableItemsCountCommand(
            $destination->getSchema(),
            $this->importState->getStagingTableName()
        ));
        $this->importState->addImportedRowsCount((int) $rows[0]['count']);
    }

    private function doIncrementalLoad(
        ImportOptions $importOptions,
        Storage\Synapse\Table $destination,
        array $primaryKeys
    ): void {
        $this->runQuery(
            $this->sqlBuilder->getBeginTransaction()
        );
        if (!empty($primaryKeys)) {
            $this->runQuery(
                $this->sqlBuilder->getUpdateWithPkCommand(
                    $destination,
                    $importOptions,
                    $this->importState->getStagingTableName(),
                    $primaryKeys
                ),
                'updateTargetTable'
            );
            $this->runQuery(
                $this->sqlBuilder->getDeleteOldItemsCommand(
                    $destination,
                    $this->importState->getStagingTableName(),
                    $primaryKeys
                ),
                'deleteUpdatedRowsFromStaging'
            );
            // dedup cannot run in transaction as it calls CREATE TABLE
            $this->runQuery(
                $this->sqlBuilder->getCommitTransaction()
            );
            $this->importState->startTimer('dedupStaging');
            $this->dedup($importOptions, $destination, $primaryKeys);
            $this->importState->stopTimer('dedupStaging');
            $this->runQuery(
                $this->sqlBuilder->getBeginTransaction()
            );
        }
        $this->runQuery(
            $this->sqlBuilder->getInsertAllIntoTargetTableCommand(
                $destination,
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
        Storage\Synapse\Table $destination,
        array $primaryKeys
    ): void {
        $tempTableName = BackendHelper::generateTempTableName();
        $this->runQuery($this->sqlBuilder->getCreateTempTableCommand(
            $destination->getSchema(),
            $tempTableName,
            $importOptions->getColumns()
        ));

        $this->runQuery(
            $this->sqlBuilder->getDedupCommand(
                $destination,
                $importOptions,
                $primaryKeys,
                $this->importState->getStagingTableName(),
                $tempTableName
            )
        );

        $this->runQuery(
            $this->sqlBuilder->getTruncateTableCommand(
                $destination->getSchema(),
                $this->importState->getStagingTableName()
            )
        );

        $this->runQuery(
            $this->sqlBuilder->getCopyTableCommand(
                $destination->getSchema(),
                $tempTableName,
                $this->importState->getStagingTableName()
            )
        );
    }

    private function doNonIncrementalLoad(
        ImportOptions $importOptions,
        Storage\Synapse\Table $destination,
        array $primaryKeys
    ): void {
        if (!empty($primaryKeys)) {
            $this->importState->startTimer('dedup');
            $this->dedup($importOptions, $destination, $primaryKeys);
            $this->importState->stopTimer('dedup');
        }

        $this->runQuery(
            $this->sqlBuilder->getTruncateTableCommand(
                $destination->getSchema(),
                $destination->getTableName()
            )
        );

        $this->runQuery(
            $this->sqlBuilder->getBeginTransaction()
        );

        $this->runQuery(
            $this->sqlBuilder->getInsertAllIntoTargetTableCommand(
                $destination,
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
