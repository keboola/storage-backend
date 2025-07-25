<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable;

use DateTimeInterface;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeException;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\ToFinalTableImporterInterface;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

final class IncrementalImporter implements ToFinalTableImporterInterface
{
    private const TIMER_DEDUP_TABLE_CREATE = 'dedupTableCreate';
    private const TIMER_UPDATE_TARGET_TABLE = 'updateTargetTable';
    private const TIMER_DELETE_UPDATED_ROWS = 'deleteUpdatedRowsFromStaging';
    private const TIMER_DEDUP_STAGING = 'dedupStaging';
    private const TIMER_INSERT_INTO_TARGET = 'insertIntoTargetFromStaging';

    private Connection $connection;

    private bool $forceUseCtas = false;

    private SqlBuilder $sqlBuilder;

    private string $timestamp;

    public function __construct(
        Connection $connection,
        ?DateTimeInterface $timestamp = null,
    ) {
        $this->connection = $connection;
        $this->sqlBuilder = new SqlBuilder();
        if ($timestamp === null) {
            $this->timestamp = DateTimeHelper::getNowFormatted();
        } else {
            $this->timestamp = DateTimeHelper::getTimestampFormated($timestamp);
        }
    }

    /**
     * This is used for testing purposes only.
     * method will be removed in the future.
     */
    public function tmpForceUseCtas(): void
    {
        $this->forceUseCtas = true;
    }

    public function importToTable(
        TableDefinitionInterface $stagingTableDefinition,
        TableDefinitionInterface $destinationTableDefinition,
        ImportOptionsInterface $options,
        ImportState $state,
    ): Result {
        assert($stagingTableDefinition instanceof SnowflakeTableDefinition);
        assert($destinationTableDefinition instanceof SnowflakeTableDefinition);
        assert($options instanceof SnowflakeImportOptions);

        // table used in getInsertAllIntoTargetTableCommand if PK's are specified, dedup table is used
        $tableToCopyFrom = $stagingTableDefinition;

        try {
            $this->connection->executeStatement(
                $this->sqlBuilder->getBeginTransaction(),
            );

            /** @var SnowflakeTableDefinition $destinationTableDefinition */
            if (!empty($destinationTableDefinition->getPrimaryKeysNames())
                && !$this->forceUseCtas
            ) {
                // has PKs for dedup

                // 0. Create table for deduplication
                $deduplicationTableDefinition = StageTableDefinitionFactory::createDedupTableDefinition(
                    $stagingTableDefinition,
                    $destinationTableDefinition->getPrimaryKeysNames(),
                );
                $tableToCopyFrom = $deduplicationTableDefinition;
                $qb = new SnowflakeTableQueryBuilder();
                $sql = $qb->getCreateTableCommandFromDefinition($deduplicationTableDefinition);
                $state->startTimer(self::TIMER_DEDUP_TABLE_CREATE);
                $this->connection->executeStatement($sql);
                $state->stopTimer(self::TIMER_DEDUP_TABLE_CREATE);

                // 1. Run UPDATE command to update rows in final table with updated data based on PKs
                $state->startTimer(self::TIMER_UPDATE_TARGET_TABLE);
                $this->connection->executeStatement(
                    $this->sqlBuilder->getUpdateWithPkCommand(
                        $stagingTableDefinition,
                        $destinationTableDefinition,
                        $options,
                        $this->timestamp,
                    ),
                );
                $state->stopTimer(self::TIMER_UPDATE_TARGET_TABLE);

                // 2. delete updated rows from staging table
                $state->startTimer(self::TIMER_DELETE_UPDATED_ROWS);
                $this->connection->executeStatement(
                    $this->sqlBuilder->getDeleteOldItemsCommand(
                        $stagingTableDefinition,
                        $destinationTableDefinition,
                        $options,
                    ),
                );
                $state->stopTimer(self::TIMER_DELETE_UPDATED_ROWS);

                // 3. dedup insert
                $state->startTimer(self::TIMER_DEDUP_STAGING);
                $this->connection->executeStatement(
                    $this->sqlBuilder->getDedupCommand(
                        $stagingTableDefinition,
                        $deduplicationTableDefinition,
                        $destinationTableDefinition->getPrimaryKeysNames(),
                    ),
                );
                $this->connection->executeStatement(
                    $this->sqlBuilder->getTruncateTable(
                        $stagingTableDefinition->getSchemaName(),
                        $stagingTableDefinition->getTableName(),
                    ),
                );
                $state->stopTimer(self::TIMER_DEDUP_STAGING);
            }

            // insert into destination table
            $state->startTimer(self::TIMER_INSERT_INTO_TARGET);
            $this->connection->executeStatement(
                $this->sqlBuilder->getInsertAllIntoTargetTableCommand(
                    $tableToCopyFrom,
                    $destinationTableDefinition,
                    $options,
                    $this->timestamp,
                ),
            );
            $state->stopTimer(self::TIMER_INSERT_INTO_TARGET);

            $this->connection->executeStatement(
                $this->sqlBuilder->getCommitTransaction(),
            );

            $state->setImportedColumns($stagingTableDefinition->getColumnsNames());
        } catch (Exception $e) {
            throw SnowflakeException::covertException($e);
        } finally {
            if (isset($deduplicationTableDefinition)) {
                // drop dedup table
                $this->connection->executeStatement(
                    $this->sqlBuilder->getDropTableIfExistsCommand(
                        $deduplicationTableDefinition->getSchemaName(),
                        $deduplicationTableDefinition->getTableName(),
                    ),
                );
            }
        }

        return $state->getResult();
    }
}
