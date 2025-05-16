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
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

final class FullImporter implements ToFinalTableImporterInterface
{
    private const TIMER_COPY_TO_TARGET = 'copyFromStagingToTarget';
    private const TIMER_CTAS_LOAD = 'ctasLoad';
    private const TIMER_DEDUP = 'fromStagingToTargetWithDedup';

    private Connection $connection;

    private SqlBuilder $sqlBuilder;

    public function __construct(
        Connection $connection,
    ) {
        $this->connection = $connection;
        $this->sqlBuilder = new SqlBuilder();
    }

    private function doFullLoadWithCTAS(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition,
        SnowflakeImportOptions $options,
        ImportState $state,
    ): void {
        // Check if staging and destination definitions match in terms of columns, datatypes, and primary keys
        $this->validateTableDefinitionsMatch($stagingTableDefinition, $destinationTableDefinition);

        $state->startTimer(self::TIMER_CTAS_LOAD);
        $this->connection->executeStatement(
            $this->sqlBuilder->getCTASInsertAllIntoTargetTableCommand(
                $stagingTableDefinition,
                $destinationTableDefinition,
            ),
        );
        $state->stopTimer(self::TIMER_CTAS_LOAD);
    }

    /**
     * Validates that the staging and destination table definitions match in terms of
     * column names, datatypes, and primary keys.
     *
     * @throws SnowflakeException If the definitions don't match
     */
    private function validateTableDefinitionsMatch(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition,
    ): void {
        // Compare column names
        $stagingColumns = $stagingTableDefinition->getColumnsNames();
        $destinationColumns = $destinationTableDefinition->getColumnsNames();

        // Filter out the _timestamp column if it exists in the destination
        $destinationColumnsFiltered = array_filter($destinationColumns, function ($column) {
            return $column !== ToStageImporterInterface::TIMESTAMP_COLUMN_NAME;
        });

        // Check if column counts match (excluding _timestamp)
        if (count($stagingColumns) !== count($destinationColumnsFiltered)) {
            throw new SnowflakeException(sprintf(
                'Column count mismatch between staging ("%d") and destination ("%d") tables',
                count($stagingColumns),
                count($destinationColumnsFiltered),
            ));
        }

        // Check if column names match
        sort($stagingColumns);
        sort($destinationColumnsFiltered);
        if ($stagingColumns !== $destinationColumnsFiltered) {
            throw new SnowflakeException(spritnf(
                'Column names do not match between staging and destination tables. Staging: "%s", Destination: "%s"',
                implode(',', $stagingColumns),
                implode(',', $destinationColumnsFiltered),
            ),
            );
        }

        // Compare primary keys
        $stagingPrimaryKeys = $stagingTableDefinition->getPrimaryKeysNames();
        $destinationPrimaryKeys = $destinationTableDefinition->getPrimaryKeysNames();
        sort($stagingPrimaryKeys);
        sort($destinationPrimaryKeys);
        if ($stagingPrimaryKeys !== $destinationPrimaryKeys) {
            throw new SnowflakeException(
                sprintf(
                    'Primary keys do not match between source and destination tables. Source: "%s", Destination: "%s"',
                    implode(',', $stagingPrimaryKeys),
                    implode(',', $destinationPrimaryKeys),
                ),
            );
        }

        // Compare column data types
        $stagingColumnDefs = $stagingTableDefinition->getColumnsDefinitions();
        $destinationColumnDefs = $destinationTableDefinition->getColumnsDefinitions();

        foreach ($stagingColumnDefs as $stagingColumn) {
            $stagingColumnName = $stagingColumn->getColumnName();
            $stagingColumnType = $stagingColumn->getColumnDefinition()->getType();

            // Find matching column in destination
            $destinationColumn = null;
            foreach ($destinationColumnDefs as $column) {
                if ($column->getColumnName() === $stagingColumnName) {
                    $destinationColumn = $column;
                    break;
                }
            }

            if ($destinationColumn === null) {
                throw new SnowflakeException(sprintf(
                    'Column "%s" exists in staging but not in destination',
                    $stagingColumnName,
                ));
            }

            // Compare data types
            $destinationColumnType = $destinationColumn->getColumnDefinition()->getType();
            if ($stagingColumnType !== $destinationColumnType) {
                throw new SnowflakeException(sprintf(
                    'Data type mismatch for column "%s": staging has "%s", destination has "%s"',
                    $stagingColumnName,
                    $stagingColumnType,
                    $destinationColumnType,
                ));
            }
        }
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
        /** @var SnowflakeTableDefinition $destinationTableDefinition */
        try {
            //import files to staging table
            if (in_array('ctas-om', $options->features())) {
                $this->doFullLoadWithCTAS(
                    $stagingTableDefinition,
                    $destinationTableDefinition,
                    $options,
                    $state,
                );
            } elseif (!empty($destinationTableDefinition->getPrimaryKeysNames())) {
                $this->doFullLoadWithDedup(
                    $stagingTableDefinition,
                    $destinationTableDefinition,
                    $options,
                    $state,
                );
            } else {
                $this->doLoadFullWithoutDedup(
                    $stagingTableDefinition,
                    $destinationTableDefinition,
                    $options,
                    $state,
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
        ImportState $state,
    ): void {
        $state->startTimer(self::TIMER_DEDUP);

        // 1. Create table for deduplication
        $deduplicationTableDefinition = StageTableDefinitionFactory::createDedupTableDefinition(
            $stagingTableDefinition,
            $destinationTableDefinition->getPrimaryKeysNames(),
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
                    $destinationTableDefinition->getPrimaryKeysNames(),
                ),
            );

            $this->connection->executeStatement(
                $this->sqlBuilder->getBeginTransaction(),
            );

            // 3 truncate destination table
            $this->connection->executeStatement(
                $this->sqlBuilder->getTruncateTable(
                    $destinationTableDefinition->getSchemaName(),
                    $destinationTableDefinition->getTableName(),
                ),
            );

            // 4 move data with INSERT INTO
            $this->connection->executeStatement(
                $this->sqlBuilder->getInsertAllIntoTargetTableCommand(
                    $deduplicationTableDefinition,
                    $destinationTableDefinition,
                    $options,
                    DateTimeHelper::getNowFormatted(),
                ),
            );
            $state->stopTimer(self::TIMER_DEDUP);
        } finally {
            // 5 drop dedup table
            $this->connection->executeStatement(
                $this->sqlBuilder->getDropTableIfExistsCommand(
                    $deduplicationTableDefinition->getSchemaName(),
                    $deduplicationTableDefinition->getTableName(),
                ),
            );
        }

        $this->connection->executeStatement(
            $this->sqlBuilder->getCommitTransaction(),
        );
    }

    private function doLoadFullWithoutDedup(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition,
        SnowflakeImportOptions $options,
        ImportState $state,
    ): void {
        $this->connection->executeStatement(
            $this->sqlBuilder->getBeginTransaction(),
        );
        // truncate destination table
        $this->connection->executeStatement(
            $this->sqlBuilder->getTruncateTable(
                $destinationTableDefinition->getSchemaName(),
                $destinationTableDefinition->getTableName(),
            ),
        );
        $state->startTimer(self::TIMER_COPY_TO_TARGET);

        // move data with INSERT INTO
        $this->connection->executeStatement(
            $this->sqlBuilder->getInsertAllIntoTargetTableCommand(
                $stagingTableDefinition,
                $destinationTableDefinition,
                $options,
                DateTimeHelper::getNowFormatted(),
            ),
        );
        $state->stopTimer(self::TIMER_COPY_TO_TARGET);

        $this->connection->executeStatement(
            $this->sqlBuilder->getCommitTransaction(),
        );
    }
}
