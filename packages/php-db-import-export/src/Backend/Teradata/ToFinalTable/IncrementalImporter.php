<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use http\Exception\RuntimeException;
use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\Teradata\Helper\DropTableTrait;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataException;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\ToFinalTableImporterInterface;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;

final class IncrementalImporter implements ToFinalTableImporterInterface
{
    private const TIMER_DEDUP_TABLE_CREATE = 'dedupTableCreate';
    private const TIMER_UPDATE_TARGET_TABLE = 'updateTargetTable';
    private const TIMER_DELETE_UPDATED_ROWS = 'deleteUpdatedRowsFromStaging';
    private const TIMER_DEDUP_STAGING = 'dedupStaging';
    private const TIMER_INSERT_INTO_TARGET = 'insertIntoTargetFromStaging';

    private Connection $connection;

    private SqlBuilder $sqlBuilder;

    use DropTableTrait;

    public function __construct(
        Connection $connection
    ) {
        $this->connection = $connection;
        $this->sqlBuilder = new SqlBuilder();
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

        // table used in getInsertAllIntoTargetTableCommand if PK's are specified, dedup table is used
        $tableToCopyFrom = $stagingTableDefinition;

        $timestampValue = DateTimeHelper::getNowFormatted();
        try {

            /** @var TeradataTableDefinition $destinationTableDefinition */
            if (!empty($destinationTableDefinition->getPrimaryKeysNames())) {
                // has PKs for dedup

                // 0. Create table for deduplication
                $deduplicationTableDefinition = StageTableDefinitionFactory::createDedupTableDefinition(
                    $stagingTableDefinition,
                    $destinationTableDefinition->getPrimaryKeysNames()
                );
                $tableToCopyFrom = $deduplicationTableDefinition;
                $qb = new TeradataTableQueryBuilder();
                $sql = $qb->getCreateTableCommandFromDefinition($deduplicationTableDefinition);
                $state->startTimer(self::TIMER_DEDUP_TABLE_CREATE);
                $this->connection->executeStatement($sql);
                $state->stopTimer(self::TIMER_DEDUP_TABLE_CREATE);

                // transaction has to start here because TD can have DDL at the end of transaction
                $this->connection->executeStatement(
                    $this->sqlBuilder->getBeginTransaction()
                );

                // 1. Run UPDATE command to update rows in final table with updated data based on PKs
                $state->startTimer(self::TIMER_UPDATE_TARGET_TABLE);
                $this->connection->executeStatement(
                    $this->sqlBuilder->getUpdateWithPkCommand(
                        $stagingTableDefinition,
                        $destinationTableDefinition,
                        $options,
                        $timestampValue
                    )
                );
                $state->stopTimer(self::TIMER_UPDATE_TARGET_TABLE);

                // 2. delete updated rows from staging table
                $state->startTimer(self::TIMER_DELETE_UPDATED_ROWS);
                $this->connection->executeStatement(
                    $this->sqlBuilder->getDeleteOldItemsCommand(
                        $stagingTableDefinition,
                        $destinationTableDefinition,
                        $options
                    )
                );
                $state->stopTimer(self::TIMER_DELETE_UPDATED_ROWS);

                // 3. dedup insert
                $state->startTimer(self::TIMER_DEDUP_STAGING);
                $this->connection->executeStatement(
                    $this->sqlBuilder->getDedupCommand(
                        $stagingTableDefinition,
                        $deduplicationTableDefinition,
                        $destinationTableDefinition->getPrimaryKeysNames()
                    )
                );
                $this->connection->executeStatement(
                    $this->sqlBuilder->getTruncateTableWithDeleteCommand(
                        $stagingTableDefinition->getSchemaName(),
                        $stagingTableDefinition->getTableName()
                    )
                );
                $state->stopTimer(self::TIMER_DEDUP_STAGING);
            } else {
                $this->connection->executeStatement(
                    $this->sqlBuilder->getBeginTransaction()
                );
            }

            // insert into destination table
            $state->startTimer(self::TIMER_INSERT_INTO_TARGET);
            $this->connection->executeStatement(
                $this->sqlBuilder->getInsertAllIntoTargetTableCommand(
                    $tableToCopyFrom,
                    $destinationTableDefinition,
                    $options,
                    $timestampValue
                )
            );
            $state->stopTimer(self::TIMER_INSERT_INTO_TARGET);

            $this->connection->executeStatement(
                $this->sqlBuilder->getEndTransaction()
            );

            $state->setImportedColumns($stagingTableDefinition->getColumnsNames());
        } catch (Exception $e) {
            throw TeradataException::covertException($e);
        } finally {
            if (isset($deduplicationTableDefinition)) {
                // 5 drop dedup table
                $this->dropTableIfExists(
                    $deduplicationTableDefinition->getSchemaName(),
                    $deduplicationTableDefinition->getTableName()
                );
            }
        }

        return $state->getResult();
    }
}
