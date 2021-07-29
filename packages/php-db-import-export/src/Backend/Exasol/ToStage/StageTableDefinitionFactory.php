<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Exasol\ToStage;

use Keboola\Datatype\Definition\Exasol;
use Keboola\Db\ImportExport\Backend\Exasol\Helper\BackendHelper;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Exasol\ExasolColumn;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;

final class StageTableDefinitionFactory
{
    /**
     * @param string[] $sourceColumnsNames
     */
    public static function createStagingTableDefinition(
        ExasolTableDefinition $destination,
        array $sourceColumnsNames
    ): ExasolTableDefinition {
        $newDefinitions = [];
        // create staging table for source columns in order
        // but with types from destination
        // also maintain source columns order
        foreach ($sourceColumnsNames as $columnName) {
            /** @var ExasolColumn $definition */
            foreach ($destination->getColumnsDefinitions() as $definition) {
                if ($definition->getColumnName() === $columnName) {
                    // if column exists in destination set destination type
                    $newDefinitions[] = new ExasolColumn(
                        $columnName,
                        new Exasol(
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

        return new ExasolTableDefinition(
            $destination->getSchemaName(),
            BackendHelper::generateTempTableName(),
            true,
            new ColumnCollection($newDefinitions),
            $destination->getPrimaryKeysNames(),
        );
    }

    /**
     * @param string[] $sourceColumnsNames
     */
    public static function createStagingTableDefinitionWithText(
        ExasolTableDefinition $destination,
        array $sourceColumnsNames
    ): ExasolTableDefinition {
        $newDefinitions = [];
        foreach ($sourceColumnsNames as $columnName) {
            $newDefinitions[] = self::createNvarcharColumn($columnName);
        }

        return new ExasolTableDefinition(
            $destination->getSchemaName(),
            BackendHelper::generateTempTableName(),
            true,
            new ColumnCollection($newDefinitions),
            $destination->getPrimaryKeysNames(),
        );
    }

    private static function createNvarcharColumn(string $columnName): ExasolColumn
    {
        return new ExasolColumn(
            $columnName,
            new Exasol(
                Exasol::TYPE_NVARCHAR,
                [
                    'length' => 2000000,
//                    'length' => Exasol::MAX_LENGTH_NVARCHAR, // TODO Exasol class doesnt have max value yet
                    'nullable' => true, // set all columns to be nullable
                ]
            )
        );
    }
}
