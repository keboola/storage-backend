<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\ToStage;

use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\ImportExport\Backend\Helper\BackendHelper;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

final class StageTableDefinitionFactory
{
    /**
     * @param string[] $sourceColumnsNames
     */
    public static function createStagingTableDefinition(
        TableDefinitionInterface $destination,
        array $sourceColumnsNames
    ): SnowflakeTableDefinition {
        /** @var SnowflakeTableDefinition $destination */
        $newDefinitions = [];
        // create staging table for source columns in order
        // but with types from destination
        // also maintain source columns order
        foreach ($sourceColumnsNames as $columnName) {
            /** @var SnowflakeColumn $definition */
            foreach ($destination->getColumnsDefinitions() as $definition) {
                if ($definition->getColumnName() === $columnName) {
                    // if column exists in destination set destination type
                    $newDefinitions[] = new SnowflakeColumn(
                        $columnName,
                        new Snowflake(
                            $definition->getColumnDefinition()->getType(),
                            [
                                'length' => $definition->getColumnDefinition()->getLength(),
                                'nullable' => true, // set all columns to be nullable
                                'default' => $definition->getColumnDefinition()->getDefault(),
                            ]
                        )
                    );
                    continue 2;
                }
            }
            // if column doesn't exists in destination set default type
            $newDefinitions[] = self::createNvarcharColumn($columnName);
        }

        return new SnowflakeTableDefinition(
            $destination->getSchemaName(),
            BackendHelper::generateStagingTableName(),
            true,
            new ColumnCollection($newDefinitions),
            $destination->getPrimaryKeysNames()
        );
    }

    private static function createNvarcharColumn(string $columnName): SnowflakeColumn
    {
        return new SnowflakeColumn(
            $columnName,
            new Snowflake(
                Snowflake::TYPE_STRING,
                [
                    // TODO max length for snowflake
                    'length' => 20000,
                    'nullable' => true, // set all columns to be nullable
                ]
            )
        );
    }

    /**
     * @param string[] $pkNames
     */
    public static function createDedupTableDefinition(
        SnowflakeTableDefinition $destination,
        array $pkNames
    ): SnowflakeTableDefinition {
        // ensure that PK on dedup table are not null
        $dedupTableColumns = [];
        /** @var SnowflakeColumn $definition */
        foreach ($destination->getColumnsDefinitions() as $definition) {
            if (in_array($definition->getColumnName(), $pkNames)) {
                $dedupTableColumns[] = new SnowflakeColumn(
                    $definition->getColumnName(),
                    new Snowflake(
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

        return new SnowflakeTableDefinition(
            $destination->getSchemaName(),
            BackendHelper::generateTempDedupTableName(),
            true,
            new ColumnCollection($dedupTableColumns),
            $pkNames
        );
    }
}
