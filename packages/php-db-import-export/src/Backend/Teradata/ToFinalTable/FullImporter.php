<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataException;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\ToFinalTableImporterInterface;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

final class FullImporter implements ToFinalTableImporterInterface
{
    private const TIMER_COPY_TO_TARGET = 'copyFromStagingToTarget';
    private const TIMER_DEDUP = 'fromStagingToTargetWithDedup';
    private const OPTIMIZED_LOAD_TMP_TABLE_SUFFIX = '_tmp';
    private const OPTIMIZED_LOAD_RENAME_TABLE_SUFFIX = '_tmp_rename';

    /** @var Connection */
    private $connection;

    /** @var SqlBuilder */
    private $sqlBuilder;

    public function __construct(
        Connection $connection
    ) {
        $this->connection = $connection;
        $this->sqlBuilder = new SqlBuilder();
    }

    private function doLoadFullWithoutDedup(
        TeradataTableDefinition $stagingTableDefinition,
        TeradataTableDefinition $destinationTableDefinition,
        TeradataImportOptions $options,
        ImportState $state
    ) {
        // truncate destination table
        $this->connection->executeStatement(
            $this->sqlBuilder->getTruncateTableWithDeleteCommand(
                $destinationTableDefinition->getSchemaName(),
                $destinationTableDefinition->getTableName()
            )
        );
        $state->startTimer(self::TIMER_COPY_TO_TARGET);

        // move data with INSERT INTO
        $sql = $this->sqlBuilder->getInsertAllIntoTargetTableCommand(
            $stagingTableDefinition,
            $destinationTableDefinition,
            $options,
            DateTimeHelper::getNowFormatted()
        );
        $this->connection->executeStatement(
            $sql
        );
        $state->stopTimer(self::TIMER_COPY_TO_TARGET);

//        $this->connection->executeStatement(
//            $this->sqlBuilder->getCommitTransaction()
//        );
    }

    public function importToTable(
        TableDefinitionInterface $stagingTableDefinition,
        TableDefinitionInterface $destinationTableDefinition,
        ImportOptionsInterface $options,
        ImportState $state
    ): Result {
        assert($stagingTableDefinition instanceof TeradataTableDefinition);
        assert($destinationTableDefinition instanceof TeradataTableDefinition);
        assert($options instanceof TeradataImportOptions);


        /** @var TeradataTableDefinition $destinationTableDefinition */
        try {
            //import files to staging table
            if (!empty($destinationTableDefinition->getPrimaryKeysNames())) {
                // dedup
                throw new \Exception('not implemented yet');
            } else {
                $this->doLoadFullWithoutDedup(
                    $stagingTableDefinition,
                    $destinationTableDefinition,
                    $options,
                    $state
                );
            }
        } catch (Exception $e) {
            throw TeradataException::covertException($e);
        } finally {
            $tmpTableName = $destinationTableDefinition->getTableName() . self::OPTIMIZED_LOAD_TMP_TABLE_SUFFIX;
            $renameTableName = $destinationTableDefinition->getTableName() . self::OPTIMIZED_LOAD_RENAME_TABLE_SUFFIX;

            if ($this->tableExists($destinationTableDefinition->getSchemaName(), $renameTableName)) {
                // drop optimized load tmp table if exists
                $this->connection->executeStatement(
                    $this->sqlBuilder->getDropTableUnsafe(
                        $destinationTableDefinition->getSchemaName(),
                        $tmpTableName
                    )
                );
            }

            if ($this->tableExists($destinationTableDefinition->getSchemaName(), $renameTableName)) {
                // drop optimized load rename table if exists
                $this->connection->executeStatement(
                    $this->sqlBuilder->getDropTableUnsafe(
                        $destinationTableDefinition->getSchemaName(),
                        $renameTableName
                    )
                );
            }
        }

        $state->setImportedColumns($stagingTableDefinition->getColumnsNames());

        return $state->getResult();
    }

    protected function tableExists($dbName, $tableName): bool
    {
        $data = $this->connection->fetchOne($this->sqlBuilder->getTableExistsCommand($dbName, $tableName));
        return ((int) $data) > 0;
    }
}
