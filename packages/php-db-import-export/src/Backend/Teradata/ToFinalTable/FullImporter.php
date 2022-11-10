<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataException;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\ToFinalTableImporterInterface;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;

final class FullImporter implements ToFinalTableImporterInterface
{
    private const TIMER_COPY_TO_TARGET = 'copyFromStagingToTarget';
    private const TIMER_DEDUP = 'fromStagingToTargetWithDedup';

    private Connection $connection;

    private SqlBuilder $sqlBuilder;

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
    ): void {
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
                $this->doFullLoadWithDedup(
                    $stagingTableDefinition,
                    $destinationTableDefinition,
                    $options,
                    $state
                );
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
        }

        $state->setImportedColumns($stagingTableDefinition->getColumnsNames());

        return $state->getResult();
    }

    protected function tableExists(string $dbName, string $tableName): bool
    {
        $data = $this->connection->fetchOne($this->sqlBuilder->getTableExistsCommand($dbName, $tableName));
        return ((int) $data) > 0;
    }

    private function doFullLoadWithDedup(
        TeradataTableDefinition $stagingTableDefinition,
        TeradataTableDefinition $destinationTableDefinition,
        TeradataImportOptions $options,
        ImportState $state
    ): void {
        $state->startTimer(self::TIMER_DEDUP);

        // 1. Create table for deduplication
        $deduplicationTableDefinition = StageTableDefinitionFactory::createDedupTableDefinition(
            $stagingTableDefinition,
            $destinationTableDefinition->getPrimaryKeysNames()
        );

        try {
            $qb = new TeradataTableQueryBuilder();
            $sql = $qb->getCreateTableCommandFromDefinition($deduplicationTableDefinition);
            $this->connection->executeStatement($sql);

            // 2 transfer data from source to dedup table with dedup process
            $this->connection->executeStatement(
                $this->sqlBuilder->getDedupCommand(
                    $stagingTableDefinition,
                    $deduplicationTableDefinition,
                    $destinationTableDefinition->getPrimaryKeysNames()
                )
            );

            $this->connection->executeStatement(
                $this->sqlBuilder->getBeginTransaction()
            );

            // 3 truncate destination table
            $this->connection->executeStatement(
                $this->sqlBuilder->getTruncateTableWithDeleteCommand(
                    $destinationTableDefinition->getSchemaName(),
                    $destinationTableDefinition->getTableName()
                )
            );

            // 4 move data with INSERT INTO
            $this->connection->executeStatement(
                $this->sqlBuilder->getInsertAllIntoTargetTableCommand(
                    $deduplicationTableDefinition,
                    $destinationTableDefinition,
                    $options,
                    DateTimeHelper::getNowFormatted()
                )
            );
            $state->stopTimer(self::TIMER_DEDUP);
        } finally {

            if ($this->tableExists(
                $deduplicationTableDefinition->getSchemaName(),
                $deduplicationTableDefinition->getTableName())
            ) {
                // 5 drop dedup table
                $this->connection->executeStatement(
                    $this->sqlBuilder->getDropTableUnsafe(
                        $deduplicationTableDefinition->getSchemaName(),
                        $deduplicationTableDefinition->getTableName()
                    )
                );
            }
        }

        $this->connection->executeStatement(
            $this->sqlBuilder->getEndTransaction()
        );
    }
}
