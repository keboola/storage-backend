<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Db\Import\Exception;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\CommandBuilder\AbsBuilder;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\File;
use Tracy\Debugger;

class Snowflake
{
    /** @var Connection */
    private $connection;

    /** @var array */
    private $timers = [];

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public function importTableFromFile(
        ImportOptions $importOptions,
        File\Azure $file
    ): void {
        $this->validateColumns($importOptions);

        //create staging table
        $stagingTableName = $this->generateStagingTableName();
        $this->createStagingTable($importOptions, $stagingTableName);

        //import files to staging table
        $this->importFileToStagingTable($importOptions, $file, $stagingTableName);
        $primaryKeys = $this->getTablesPrimaryKeys($importOptions);
        if ($importOptions->isIncremental()) {
            $this->doIncrementalLoad($importOptions, $stagingTableName, $primaryKeys);
        } else {
            $this->doNonIncrementalLoad($importOptions, $stagingTableName, $primaryKeys);
        }
        $this->runQuery(
            AbsBuilder::buildDropCommand($importOptions->getSchema(), $stagingTableName)
        );
    }

    private function doNonIncrementalLoad(
        ImportOptions $importOptions,
        string $stagingTableName,
        array $primaryKeys
    ): void {
        if (!empty($primaryKeys)){
            $tempTableName = $this->generateStagingTableName();
            $this->createStagingTable($importOptions, $tempTableName);
            $this->runQuery(
                AbsBuilder::buildDedupCommand($importOptions, $primaryKeys, $stagingTableName, $tempTableName)
            );
            $this->runQuery(
                AbsBuilder::buildDropCommand($importOptions->getSchema(), $importOptions->getTableName())
            );
            $this->runQuery(
                AbsBuilder::buildRenameTableCommand($importOptions->getSchema(), $stagingTableName, $tempTableName)
            );
        }
        $this->runQuery(
            AbsBuilder::buildBeginTransactionCommand()
        );
        $this->runQuery(
            AbsBuilder::buildTruncateTableCommand($importOptions->getSchema(), $importOptions->getTableName())
        );
        $this->runQuery(
            AbsBuilder::buildInsertAllIntoTargetTableCommand($importOptions, $stagingTableName)
        );
        $this->runQuery(
            AbsBuilder::buildCommitTransactionCommand()
        );
    }

    private function doIncrementalLoad(ImportOptions $importOptions, string $stagingTableName, array $primaryKeys): void
    {
        if (!empty($primaryKeys)) {
            $this->runQuery(
                AbsBuilder::buildBeginTransactionCommand()
            );
            $this->runQuery(
                AbsBuilder::buildUpdateWithPkCommand($importOptions, $stagingTableName, $primaryKeys)
            );
            $this->runQuery(
                AbsBuilder::buildDeleteOldItemsCommand($importOptions, $stagingTableName, $primaryKeys)
            );
            $tempTableName = $this->generateStagingTableName();
            $this->createStagingTable($importOptions, $tempTableName);
            $this->runQuery(
                AbsBuilder::buildDedupCommand($importOptions, $primaryKeys, $stagingTableName, $tempTableName)
            );
            $this->runQuery(
                AbsBuilder::buildDropCommand(
                    $importOptions->getSchema(),
                    $stagingTableName
                )
            );
            $this->runQuery(
                AbsBuilder::buildRenameTableCommand(
                    $importOptions->getSchema(),
                    $stagingTableName,
                    $tempTableName
                )
            );
        }
        $this->runQuery(
            AbsBuilder::buildInsertFromStagingToTargetTableCommand($importOptions, $stagingTableName)
        );
        $this->runQuery(
            AbsBuilder::buildCommitTransactionCommand()
        );
        $this->runQuery(
            AbsBuilder::buildDropCommand(
                $importOptions->getSchema(),
                $importOptions->getTableName()
            )
        );
    }

    private function getTablesPrimaryKeys(ImportOptions $importOptions): array
    {
        return $this->connection->getTablePrimaryKey(
            $importOptions->getSchema(),
            $importOptions->getTableName()
        );
    }

    private function importFileToStagingTable(
        ImportOptions $importOptions,
        File\Azure $file,
        string $stagingTableName
    ): void {
        $commands = AbsBuilder::buildFileCopyCommands($importOptions, $file, $stagingTableName);
        foreach ($commands as $command) {
            $this->runQuery($command);
        }
    }

    private function createStagingTable(ImportOptions $importOptions, string $stagingTableName): void
    {
        $this->runQuery(AbsBuilder::buildCreateStagingTableCommand(
            $importOptions->getSchema(),
            $stagingTableName,
            $importOptions->getColumns()
        ));
    }

    private function generateStagingTableName(): string
    {
        return '__temp_' . str_replace('.', '_', uniqid('csvimport', true));
    }

    private function runQuery(string $query, ?string $timerName = null): void
    {
        if ($timerName) {
            Debugger::timer($timerName);
        }
        // echo sprintf("Executing query: %s \n", $query);
        $this->connection->query($query);
        if ($timerName) {
            $this->timers[] = [
                'name' => $timerName,
                'durationSeconds' => Debugger::timer($timerName),
            ];
        }
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
}
