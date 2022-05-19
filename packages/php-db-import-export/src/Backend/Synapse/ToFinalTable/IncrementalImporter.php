<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable;

use Doctrine\DBAL\Connection;
use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\ToFinalTableImporterInterface;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

final class IncrementalImporter implements ToFinalTableImporterInterface
{
    private const TIMER_DEDUP_TABLE_CREATE = 'dedupTableCreate';
    private const TIMER_UPDATE_TARGET_TABLE = 'updateTargetTable';
    private const TIMER_DELETE_UPDATED_ROWS = 'deleteUpdatedRowsFromStaging';
    private const TIMER_DEDUP_STAGING = 'dedupStaging';
    private const TIMER_INSERT_INTO_TARGET = 'insertIntoTargetFromStaging';

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
        assert($stagingTableDefinition instanceof SynapseTableDefinition);
        assert($destinationTableDefinition instanceof SynapseTableDefinition);
        assert($options instanceof SynapseImportOptions);

        // table used in getInsertAllIntoTargetTableCommand if PK's are specified, dedup table is used
        $tableToCopyFrom = $stagingTableDefinition;

        $timestampValue = DateTimeHelper::getNowFormatted();
        if (!empty($destinationTableDefinition->getPrimaryKeysNames())) {
            // Create table for deduplication
            $deduplicationTableDefinition = StageTableDefinitionFactory::createStagingTableDefinition(
                $stagingTableDefinition,
                $stagingTableDefinition->getColumnsNames(),
                $destinationTableDefinition->getTableIndex()
            );
            $tableToCopyFrom = $deduplicationTableDefinition;
            $qb = new SynapseTableQueryBuilder();
            $sql = $qb->getCreateTableCommandFromDefinition($deduplicationTableDefinition);
            $state->startTimer(self::TIMER_DEDUP_TABLE_CREATE);
            $this->connection->executeStatement($sql);
            $state->stopTimer(self::TIMER_DEDUP_TABLE_CREATE);

            $this->connection->executeStatement(
                $this->sqlBuilder->getBeginTransaction()
            );

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

            $state->startTimer(self::TIMER_DELETE_UPDATED_ROWS);
            $this->connection->executeStatement(
                $this->sqlBuilder->getDeleteOldItemsCommand(
                    $stagingTableDefinition,
                    $destinationTableDefinition
                )
            );

            $state->stopTimer(self::TIMER_DELETE_UPDATED_ROWS);

            $state->startTimer(self::TIMER_DEDUP_STAGING);
            $this->connection->executeStatement(
                $this->sqlBuilder->getDedupCommand(
                    $stagingTableDefinition,
                    $deduplicationTableDefinition,
                    $destinationTableDefinition->getPrimaryKeysNames()
                )
            );

            $state->stopTimer(self::TIMER_DEDUP_STAGING);
        } else {
            $this->connection->executeStatement(
                $this->sqlBuilder->getBeginTransaction()
            );
        }

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
            $this->sqlBuilder->getCommitTransaction()
        );

        $state->setImportedColumns($stagingTableDefinition->getColumnsNames());

        return $state->getResult();
    }
}
