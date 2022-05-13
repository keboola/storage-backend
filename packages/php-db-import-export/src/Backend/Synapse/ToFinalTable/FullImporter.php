<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable;

use Doctrine\DBAL\Connection;
use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\Synapse\Helper\BackendHelper;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\Backend\ToFinalTableImporterInterface;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\TableBackendUtils\Schema\SynapseSchemaReflection;
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

final class FullImporter implements ToFinalTableImporterInterface
{
    private const TIMER_DEDUP_CTAS = 'CTAS_dedup';
    private const TIMER_COPY_TO_TARGET = 'copyFromStagingToTarget';
    private const OPTIMIZED_LOAD_TMP_TABLE_SUFFIX = '_tmp';
    private const OPTIMIZED_LOAD_RENAME_TABLE_SUFFIX = '_tmp_rename';

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

        $random = BackendHelper::generateRandomTablePrefix();
        try {
            //import files to staging table
            if (!empty($destinationTableDefinition->getPrimaryKeysNames())) {
                $this->doFullLoadWithDedup(
                    $stagingTableDefinition,
                    $destinationTableDefinition,
                    $options,
                    $state,
                    $random
                );
            } else {
                $this->doLoadFullWithoutDedup(
                    $stagingTableDefinition,
                    $destinationTableDefinition,
                    $options,
                    $state,
                    $random
                );
            }
        } finally {
            // drop optimized load tmp table if exists
            $this->connection->executeStatement(
                $this->sqlBuilder->getDropTableIfExistsCommand(
                    $destinationTableDefinition->getSchemaName(),
                    $destinationTableDefinition->getTableName() . $random . self::OPTIMIZED_LOAD_TMP_TABLE_SUFFIX
                )
            );
            // drop optimized load rename table if exists
            $this->connection->executeStatement(
                $this->sqlBuilder->getDropTableIfExistsCommand(
                    $destinationTableDefinition->getSchemaName(),
                    $destinationTableDefinition->getTableName() . $random . self::OPTIMIZED_LOAD_RENAME_TABLE_SUFFIX
                )
            );
        }

        $state->setImportedColumns($stagingTableDefinition->getColumnsNames());

        return $state->getResult();
    }

    private function doFullLoadWithDedup(
        SynapseTableDefinition $stagingTableDefinition,
        SynapseTableDefinition $destinationTableDefinition,
        SynapseImportOptions $options,
        ImportState $state,
        string $random
    ): void {
        $tmpDestination = new SynapseTableDefinition(
            $destinationTableDefinition->getSchemaName(),
            $destinationTableDefinition->getTableName() . $random . self::OPTIMIZED_LOAD_TMP_TABLE_SUFFIX,
            false,
            $destinationTableDefinition->getColumnsDefinitions(),
            $destinationTableDefinition->getPrimaryKeysNames(),
            $destinationTableDefinition->getTableDistribution(),
            $destinationTableDefinition->getTableIndex()
        );

        $state->startTimer(self::TIMER_DEDUP_CTAS);
        $this->connection->executeStatement(
            $this->sqlBuilder->getCtasDedupCommand(
                $stagingTableDefinition,
                $tmpDestination,
                $options,
                DateTimeHelper::getNowFormatted()
            )
        );
        $state->stopTimer(self::TIMER_DEDUP_CTAS);

        $tmpDestinationToRemove = $destinationTableDefinition->getTableName()
            . $random . self::OPTIMIZED_LOAD_RENAME_TABLE_SUFFIX;

        try {
            $this->connection->executeStatement(
                $this->sqlBuilder->getRenameTableCommand(
                    $destinationTableDefinition->getSchemaName(),
                    $destinationTableDefinition->getTableName(),
                    $tmpDestinationToRemove
                )
            );
            $this->connection->executeStatement(
                $this->sqlBuilder->getRenameTableCommand(
                    $tmpDestination->getSchemaName(),
                    $tmpDestination->getTableName(),
                    $destinationTableDefinition->getTableName()
                )
            );
        } catch (\Throwable $e) {
            if (!$this->isTableInSchema(
                $destinationTableDefinition->getSchemaName(),
                $destinationTableDefinition->getTableName()
            )) {
                // in case of error ensure original table is renamed back
                $this->connection->executeStatement(
                    $this->sqlBuilder->getRenameTableCommand(
                        $destinationTableDefinition->getSchemaName(),
                        $tmpDestinationToRemove,
                        $destinationTableDefinition->getTableName()
                    )
                );
            }
            throw $e;
        }

        $this->connection->executeStatement(
            $this->sqlBuilder->getDropCommand(
                $destinationTableDefinition->getSchemaName(),
                $tmpDestinationToRemove
            )
        );
    }

    private function doLoadFullWithoutDedup(
        SynapseTableDefinition $stagingTableDefinition,
        SynapseTableDefinition $destinationTableDefinition,
        SynapseImportOptions $options,
        ImportState $state,
        string $random
    ): void {
        $tmpDestination = new SynapseTableDefinition(
            $destinationTableDefinition->getSchemaName(),
            $destinationTableDefinition->getTableName() . $random . self::OPTIMIZED_LOAD_TMP_TABLE_SUFFIX,
            false,
            $destinationTableDefinition->getColumnsDefinitions(),
            $destinationTableDefinition->getPrimaryKeysNames(),
            $destinationTableDefinition->getTableDistribution(),
            $destinationTableDefinition->getTableIndex()
        );

        $state->startTimer(self::TIMER_COPY_TO_TARGET);
        $this->connection->executeStatement(
            $this->sqlBuilder->getCTASInsertAllIntoTargetTableCommand(
                $stagingTableDefinition,
                $tmpDestination,
                $options,
                DateTimeHelper::getNowFormatted()
            )
        );
        $state->stopTimer(self::TIMER_COPY_TO_TARGET);

        $tmpDestinationToRemove = $destinationTableDefinition->getTableName()
            . $random . self::OPTIMIZED_LOAD_RENAME_TABLE_SUFFIX;

        try {
            $this->connection->executeStatement(
                $this->sqlBuilder->getRenameTableCommand(
                    $destinationTableDefinition->getSchemaName(),
                    $destinationTableDefinition->getTableName(),
                    $tmpDestinationToRemove
                )
            );
            $this->connection->executeStatement(
                $this->sqlBuilder->getRenameTableCommand(
                    $tmpDestination->getSchemaName(),
                    $tmpDestination->getTableName(),
                    $destinationTableDefinition->getTableName()
                )
            );
        } catch (\Throwable $e) {
            if (!$this->isTableInSchema(
                $destinationTableDefinition->getSchemaName(),
                $destinationTableDefinition->getTableName()
            )) {
                // in case of error ensure original table is renamed back
                $this->connection->executeStatement(
                    $this->sqlBuilder->getRenameTableCommand(
                        $destinationTableDefinition->getSchemaName(),
                        $tmpDestinationToRemove,
                        $destinationTableDefinition->getTableName()
                    )
                );
            }
            throw $e;
        }

        $this->connection->executeStatement(
            $this->sqlBuilder->getDropCommand(
                $destinationTableDefinition->getSchemaName(),
                $tmpDestinationToRemove
            )
        );
    }

    private function isTableInSchema(string $schemaName, string $tableName): bool
    {
        return in_array(
            $tableName,
            (new SynapseSchemaReflection($this->connection, $schemaName))->getTablesNames(),
            true
        );
    }
}
