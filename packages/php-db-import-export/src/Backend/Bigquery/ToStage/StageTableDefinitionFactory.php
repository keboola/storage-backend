<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\ToStage;

use Keboola\Datatype\Definition\Bigquery;
use Keboola\Db\ImportExport\Backend\Helper\BackendHelper;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;

final class StageTableDefinitionFactory
{
    /**
     * @param string[] $sourceColumnsNames
     */
    public static function createStagingTableDefinition(
        BigqueryTableDefinition $destination,
        array $sourceColumnsNames,
    ): BigqueryTableDefinition {
        $newDefinitions = [];
        // create staging table for source columns in order
        // but with types from destination
        // also maintain source columns order
        foreach ($sourceColumnsNames as $columnName) {
            /** @var BigqueryColumn $definition */
            foreach ($destination->getColumnsDefinitions() as $definition) {
                if ($definition->getColumnName() === $columnName) {
                    // if column exists in destination set destination type
                    $newDefinitions[] = new BigqueryColumn(
                        $columnName,
                        new Bigquery(
                            $definition->getColumnDefinition()->getType(),
                            [
                                'length' => $definition->getColumnDefinition()->getLength(),
                                'nullable' => true,
                                'default' => $definition->getColumnDefinition()->getDefault(),
                            ],
                        ),
                    );
                    continue 2;
                }
            }
            // if column doesn't exists in destination set default type
            $newDefinitions[] = self::createVarcharColumn($columnName);
        }

        return new BigqueryTableDefinition(
            $destination->getSchemaName(),
            BackendHelper::generateStagingTableName(),
            true,
            new ColumnCollection($newDefinitions),
            $destination->getPrimaryKeysNames(),
        );
    }

    private static function createVarcharColumn(string $columnName): BigqueryColumn
    {
        return new BigqueryColumn(
            $columnName,
            new Bigquery(
                Bigquery::TYPE_STRING,
                [
                    'nullable' => true,
                ],
            ),
        );
    }

    /**
     * @param string[] $sourceColumnsNames
     */
    public static function createStagingTableDefinitionWithText(
        BigqueryTableDefinition $destination,
        array $sourceColumnsNames,
    ): BigqueryTableDefinition {
        $newDefinitions = [];
        foreach ($sourceColumnsNames as $columnName) {
            $newDefinitions[] = self::createVarcharColumn($columnName);
        }

        return new BigqueryTableDefinition(
            $destination->getSchemaName(),
            BackendHelper::generateStagingTableName(),
            true,
            new ColumnCollection($newDefinitions),
            $destination->getPrimaryKeysNames(),
        );
    }
}
