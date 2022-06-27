<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse\ToStage;

use Keboola\Datatype\Definition\Synapse;
use Keboola\Db\ImportExport\Backend\Synapse\Helper\BackendHelper;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\Table\Synapse\TableDistributionDefinition;
use Keboola\TableBackendUtils\Table\Synapse\TableIndexDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;

final class StageTableDefinitionFactory
{
    /**
     * @param string[] $sourceColumnsNames
     */
    public static function createStagingTableDefinition(
        SynapseTableDefinition $destination,
        array $sourceColumnsNames,
        ?TableIndexDefinition $indexDefinition = null
    ): SynapseTableDefinition {
        $clusteredIndexColumns = self::getClusteredIndexColumns($indexDefinition);

        $newDefinitions = [];
        // create staging table for source columns in order
        // but with types from destination
        // also maintain source columns order
        foreach ($sourceColumnsNames as $columnName) {
            /** @var SynapseColumn $definition */
            foreach ($destination->getColumnsDefinitions() as $definition) {
                if ($definition->getColumnName() === $columnName) {
                    $isNullable = $clusteredIndexColumns === null
                        || !in_array($columnName, $clusteredIndexColumns, true);
                    // if column exists in destination set destination type
                    $newDefinitions[] = new SynapseColumn(
                        $columnName,
                        new Synapse(
                            $definition->getColumnDefinition()->getType(),
                            [
                                'length' => $definition->getColumnDefinition()->getLength(),
                                // set all columns to be nullable except in clustered index
                                'nullable' => $isNullable,
                                'default' => $definition->getColumnDefinition()->getDefault(),
                            ]
                        )
                    );
                    continue 2;
                }
            }
            // if column doesn't exists in destination set default type
            $newDefinitions[] = self::createNvarcharColumn($columnName, $clusteredIndexColumns);
        }

        return new SynapseTableDefinition(
            $destination->getSchemaName(),
            BackendHelper::generateTempTableName(),
            true,
            new ColumnCollection($newDefinitions),
            $destination->getPrimaryKeysNames(),
            self::resolveDistribution($destination->getTableDistribution()),
            $indexDefinition ?? new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );
    }

    /**
     * @param string[]|null $clusteredIndexColumns
     */
    private static function createNvarcharColumn(string $columnName, ?array $clusteredIndexColumns): SynapseColumn
    {
        $isNullable = $clusteredIndexColumns === null
            || !in_array($columnName, $clusteredIndexColumns, true);
        return new SynapseColumn(
            $columnName,
            new Synapse(
                Synapse::TYPE_NVARCHAR,
                [
                    'length' => (string) Synapse::MAX_LENGTH_NVARCHAR,
                    // set all columns to be nullable except in clustered index
                    'nullable' => $isNullable,
                ]
            )
        );
    }

    /**
     * @param string[] $sourceColumnsNames
     */
    public static function createStagingTableDefinitionWithText(
        SynapseTableDefinition $destination,
        array $sourceColumnsNames,
        ?TableIndexDefinition $indexDefinition = null
    ): SynapseTableDefinition {
        $clusteredIndexColumns = self::getClusteredIndexColumns($indexDefinition);

        $newDefinitions = [];
        foreach ($sourceColumnsNames as $columnName) {
            $newDefinitions[] = self::createNvarcharColumn($columnName, $clusteredIndexColumns);
        }

        return new SynapseTableDefinition(
            $destination->getSchemaName(),
            BackendHelper::generateTempTableName(),
            true,
            new ColumnCollection($newDefinitions),
            $destination->getPrimaryKeysNames(),
            self::resolveDistribution($destination->getTableDistribution()),
            $indexDefinition ?? new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );
    }

    /**
     * @return string[]|null
     */
    private static function getClusteredIndexColumns(?TableIndexDefinition $indexDefinition): ?array
    {
        $isClusteredIndex = $indexDefinition !== null
            && $indexDefinition->getIndexType() === TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_INDEX;
        if ($isClusteredIndex) {
            return $indexDefinition->getIndexedColumnsNames();
        }
        return null;
    }

    private static function resolveDistribution(
        TableDistributionDefinition $distribution
    ): TableDistributionDefinition {
        //phpcs:ignore
        $isReplicateTable = $distribution->getDistributionName() === TableDistributionDefinition::TABLE_DISTRIBUTION_REPLICATE;
        if ($isReplicateTable) {
            // If table distribution is REPLICATE use ROUND_ROBIN as distribution for temp tables can't be REPLICATE
            return new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN);
        }
        return $distribution;
    }
}
