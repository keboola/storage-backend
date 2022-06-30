<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\ToStage;

use Keboola\Datatype\Definition\Teradata;
use Keboola\Db\ImportExport\Backend\Teradata\Helper\BackendHelper;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;

final class StageTableDefinitionFactory
{
    /**
     * @param string[] $sourceColumnsNames
     */
    public static function createStagingTableDefinition(
        TeradataTableDefinition $destination,
        array $sourceColumnsNames
    ): TeradataTableDefinition {
        $newDefinitions = [];
        // create staging table for source columns in order
        // but with types from destination
        // also maintain source columns order
        foreach ($sourceColumnsNames as $columnName) {
            /** @var TeradataColumn $definition */
            foreach ($destination->getColumnsDefinitions() as $definition) {
                if ($definition->getColumnName() === $columnName) {
                    // if column exists in destination set destination type
                    $newDefinitions[] = new TeradataColumn(
                        $columnName,
                        new Teradata(
                            $definition->getColumnDefinition()->getType(),
                            [
                                'length' => $definition->getColumnDefinition()->getLength(),
                                'nullable' => true,
                                'default' => $definition->getColumnDefinition()->getDefault(),
                            ]
                        )
                    );
                    continue 2;
                }
            }
            // if column doesn't exists in destination set default type
            $newDefinitions[] = self::createVarcharColumn($columnName);
        }

        return new TeradataTableDefinition(
            $destination->getSchemaName(),
            BackendHelper::generateStagingTableName(),
            true,
            new ColumnCollection($newDefinitions),
            $destination->getPrimaryKeysNames()
        );
    }

    private static function createVarcharColumn(string $columnName): TeradataColumn
    {
        return new TeradataColumn(
            $columnName,
            new Teradata(
                Teradata::TYPE_VARCHAR,
                [
                    'length' => (string) Teradata::DEFAULT_NON_LATIN_CHAR_LENGTH,
                    'nullable' => true,
                ]
            )
        );
    }

    /**
     * @param string[] $sourceColumnsNames
     */
    public static function createStagingTableDefinitionWithText(
        TeradataTableDefinition $destination,
        array $sourceColumnsNames
    ): TeradataTableDefinition {
        $newDefinitions = [];
        foreach ($sourceColumnsNames as $columnName) {
            $newDefinitions[] = self::createVarcharColumn($columnName);
        }

        return new TeradataTableDefinition(
            $destination->getSchemaName(),
            BackendHelper::generateStagingTableName(),
            true,
            new ColumnCollection($newDefinitions),
            $destination->getPrimaryKeysNames()
        );
    }
}
