<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable;

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

    public function importToTable(
        TableDefinitionInterface $stagingTableDefinition,
        TableDefinitionInterface $destinationTableDefinition,
        ImportOptionsInterface $options,
        ImportState $state
    ): Result {
        assert($stagingTableDefinition instanceof SnowflakeTableDefinition);
        assert($destinationTableDefinition instanceof SnowflakeTableDefinition);
        assert($options instanceof SnowflakeImportOptions);
        /** @var SnowflakeTableDefinition $destinationTableDefinition */
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
            throw SnowflakeException::covertException($e);
        }

        $state->setImportedColumns($stagingTableDefinition->getColumnsNames());

        return $state->getResult();
    }

    private function doFullLoadWithDedup(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition,
        SnowflakeImportOptions $options,
        ImportState $state
    ): void {

        $this->connection->executeStatement(
            $this->sqlBuilder->getBeginTransaction()
        );

        $state->startTimer(self::TIMER_DEDUP);

        // 1. Create table for deduplication
        $deduplicationTableDefinition = StageTableDefinitionFactory::createDedupTableDefinition(
            $stagingTableDefinition,
            $destinationTableDefinition->getPrimaryKeysNames()
        );

        try {
            $qb = new SnowflakeTableQueryBuilder();
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

            // 3 truncate destination table
            $this->connection->executeStatement(
                $this->sqlBuilder->getTruncateTable(
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
            // 5 drop dedup table
            $this->connection->executeStatement(
                $this->sqlBuilder->getDropTableIfExistsCommand(
                    $deduplicationTableDefinition->getSchemaName(),
                    $deduplicationTableDefinition->getTableName()
                )
            );
        }

        $this->connection->executeStatement(
            $this->sqlBuilder->getCommitTransaction()
        );
    }

    private function doLoadFullWithoutDedup(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition,
        SnowflakeImportOptions $options,
        ImportState $state
    ): void {
        $this->connection->executeStatement(
            $this->sqlBuilder->getBeginTransaction()
        );
        // truncate destination table
        $this->connection->executeStatement(
            $this->sqlBuilder->getTruncateTable(
                $destinationTableDefinition->getSchemaName(),
                $destinationTableDefinition->getTableName()
            )
        );
        $state->startTimer(self::TIMER_COPY_TO_TARGET);

        // move data with INSERT INTO
        $this->connection->executeStatement(
            $this->sqlBuilder->getInsertAllIntoTargetTableCommand(
                $stagingTableDefinition,
                $destinationTableDefinition,
                $options,
                DateTimeHelper::getNowFormatted()
            )
        );
        $state->stopTimer(self::TIMER_COPY_TO_TARGET);

        $this->connection->executeStatement(
            $this->sqlBuilder->getCommitTransaction()
        );
    }
}
