<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Exasol\ToFinalTable;

use Doctrine\DBAL\Connection;
use Keboola\Datatype\Definition\Exasol;
use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Exasol\Helper\BackendHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
use Keboola\Db\ImportExport\Backend\ToFinalTableImporterInterface;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Exasol\ExasolColumn;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableReflection;
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

    public function importToTable(
        TableDefinitionInterface $stagingTableDefinition,
        TableDefinitionInterface $destinationTableDefinition,
        ImportOptionsInterface $options,
        ImportState $state
    ): Result {
        assert($stagingTableDefinition instanceof ExasolTableDefinition);
        assert($destinationTableDefinition instanceof ExasolTableDefinition);
        assert($options instanceof ExasolImportOptions);
        /** @var ExasolTableDefinition $destinationTableDefinition */
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
        } finally {
            // drop optimized load tmp table if exists
            $this->connection->executeStatement(
                $this->sqlBuilder->getDropTableIfExistsCommand(
                    $destinationTableDefinition->getSchemaName(),
                    $destinationTableDefinition->getTableName() . self::OPTIMIZED_LOAD_TMP_TABLE_SUFFIX
                )
            );
            // drop optimized load rename table if exists
            $this->connection->executeStatement(
                $this->sqlBuilder->getDropTableIfExistsCommand(
                    $destinationTableDefinition->getSchemaName(),
                    $destinationTableDefinition->getTableName() . self::OPTIMIZED_LOAD_RENAME_TABLE_SUFFIX
                )
            );
        }

        $state->setImportedColumns($stagingTableDefinition->getColumnsNames());

        return $state->getResult();
    }

    private function doFullLoadWithDedup(
        ExasolTableDefinition $stagingTableDefinition,
        ExasolTableDefinition $destinationTableDefinition,
        ExasolImportOptions $options,
        ImportState $state
    ): void {
        $state->startTimer(self::TIMER_DEDUP);

        // ensure that PK on dedup table are not null
        $dedupTableColumns = [];
        /** @var ExasolColumn $definition */
        foreach ($stagingTableDefinition->getColumnsDefinitions() as $definition) {
            if (in_array($definition->getColumnName(), $destinationTableDefinition->getPrimaryKeysNames())) {
                $dedupTableColumns[] = new ExasolColumn(
                    $definition->getColumnName(),
                    new Exasol(
                        $definition->getColumnDefinition()->getType(),
                        [
                            'length' => $definition->getColumnDefinition()->getLength(),
                            'nullable' => false,
                        ]
                    )
                );
            } else {
                $dedupTableColumns[] = $definition;
            }
        }

        // 1 create dedup table
        $dedupTmpTableName = BackendHelper::generateTempDedupTableName();
        $this->connection->executeStatement((new ExasolTableQueryBuilder())->getCreateTableCommand(
            $destinationTableDefinition->getSchemaName(),
            $dedupTmpTableName,
            new ColumnCollection($dedupTableColumns),
            $destinationTableDefinition->getPrimaryKeysNames()
        ));
        $dedupTableRef = new ExasolTableReflection(
            $this->connection,
            $destinationTableDefinition->getSchemaName(),
            $dedupTmpTableName
        );

        /** @var ExasolTableDefinition $dedupTableDef */
        $dedupTableDef = $dedupTableRef->getTableDefinition();

        // 2 transfer data from source to dedup table with dedup process
        $this->connection->executeStatement(
            $this->sqlBuilder->getDedupCommand(
                $stagingTableDefinition,
                $dedupTableDef,
                $destinationTableDefinition->getPrimaryKeysNames()
            )
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
                $dedupTableDef,
                $destinationTableDefinition,
                $options,
                DateTimeHelper::getNowFormatted()
            )
        );
        $state->stopTimer(self::TIMER_DEDUP);

        // 5 drop dedup table
        $this->sqlBuilder->getDropTableIfExistsCommand($dedupTableDef->getSchemaName(), $dedupTableDef->getTableName());

        $this->connection->executeStatement(
            $this->sqlBuilder->getCommitTransaction()
        );
    }

    private function doLoadFullWithoutDedup(
        ExasolTableDefinition $stagingTableDefinition,
        ExasolTableDefinition $destinationTableDefinition,
        ExasolImportOptions $options,
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
