<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Keboola\Db\Import\Exception;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\BackendHelper;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\SourceStorage;
use Tracy\Debugger;

class Importer implements ImporterInterface
{
    /** @var Connection */
    private $connection;

    /** @var array */
    private $timers = [];

    public function __construct(
        Connection $connection
    ) {
        $this->connection = $connection;
    }

    public function importTable(
        ImportOptions $options,
        SourceStorage\SourceInterface $source
    ): void {
        $this->validateColumns($options);

        //create staging table
        $stagingTableName = BackendHelper::generateStagingTableName();
        $this->createStagingTable($options, $stagingTableName);

        //import files to staging table
        $this->importFileToStagingTable($options, $source, $stagingTableName);
        $primaryKeys = $this->getTablesPrimaryKeys($options);
        if ($options->isIncremental()) {
            $this->doIncrementalLoad($options, $stagingTableName, $primaryKeys);
        } else {
            $this->doNonIncrementalLoad($options, $stagingTableName, $primaryKeys);
        }
        $this->runQuery(
            CommandGeneratorHelper::buildDropCommand($options->getSchema(), $stagingTableName)
        );
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

    private function createStagingTable(
        ImportOptions $importOptions,
        string $stagingTableName
    ): void {
        $this->runQuery(CommandGeneratorHelper::buildCreateStagingTableCommand(
            $importOptions->getSchema(),
            $stagingTableName,
            $importOptions->getColumns()
        ));
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

    private function importFileToStagingTable(
        ImportOptions $importOptions,
        SourceStorage\SourceInterface $source,
        string $stagingTableName
    ): void {
        $commands = ($source->getBackendAdapter($this))
            ->getCopyCommands($importOptions, $stagingTableName);

        foreach ($commands as $command) {
            $this->runQuery($command);
        }
    }

    private function getTablesPrimaryKeys(ImportOptions $importOptions): array
    {
        return $this->connection->getTablePrimaryKey(
            $importOptions->getSchema(),
            $importOptions->getTableName()
        );
    }

    private function doIncrementalLoad(
        ImportOptions $importOptions,
        string $stagingTableName,
        array $primaryKeys
    ): void {
        if (!empty($primaryKeys)) {
            $this->runQuery(
                CommandGeneratorHelper::buildBeginTransactionCommand()
            );
            $this->runQuery(
                CommandGeneratorHelper::buildUpdateWithPkCommand($importOptions, $stagingTableName, $primaryKeys)
            );
            $this->runQuery(
                CommandGeneratorHelper::buildDeleteOldItemsCommand($importOptions, $stagingTableName, $primaryKeys)
            );
            $tempTableName = BackendHelper::generateStagingTableName();
            $this->createStagingTable($importOptions, $tempTableName);
            $this->runQuery(
                CommandGeneratorHelper::buildDedupCommand(
                    $importOptions,
                    $primaryKeys,
                    $stagingTableName,
                    $tempTableName
                )
            );
            $this->runQuery(
                CommandGeneratorHelper::buildDropCommand(
                    $importOptions->getSchema(),
                    $stagingTableName
                )
            );
            $this->runQuery(
                CommandGeneratorHelper::buildRenameTableCommand(
                    $importOptions->getSchema(),
                    $tempTableName,
                    $stagingTableName
                )
            );
        }
        $this->runQuery(
            CommandGeneratorHelper::buildInsertFromStagingToTargetTableCommand($importOptions, $stagingTableName)
        );
        $this->runQuery(
            CommandGeneratorHelper::buildCommitTransactionCommand()
        );
    }

    private function doNonIncrementalLoad(
        ImportOptions $importOptions,
        string $stagingTableName,
        array $primaryKeys
    ): void {
        if (!empty($primaryKeys)) {
            $tempTableName = BackendHelper::generateStagingTableName();
            $this->createStagingTable($importOptions, $tempTableName);
            $this->runQuery(
                CommandGeneratorHelper::buildDedupCommand(
                    $importOptions,
                    $primaryKeys,
                    $stagingTableName,
                    $tempTableName
                )
            );
            $this->runQuery(
                CommandGeneratorHelper::buildDropCommand(
                    $importOptions->getSchema(),
                    $stagingTableName
                )
            );
            $this->runQuery(
                CommandGeneratorHelper::buildRenameTableCommand(
                    $importOptions->getSchema(),
                    $tempTableName,
                    $stagingTableName
                )
            );
        }
        $this->runQuery(
            CommandGeneratorHelper::buildBeginTransactionCommand()
        );
        $this->runQuery(
            CommandGeneratorHelper::buildTruncateTableCommand(
                $importOptions->getSchema(),
                $importOptions->getTableName()
            )
        );
        $this->runQuery(
            CommandGeneratorHelper::buildInsertAllIntoTargetTableCommand($importOptions, $stagingTableName)
        );
        $this->runQuery(
            CommandGeneratorHelper::buildCommitTransactionCommand()
        );
    }
}
