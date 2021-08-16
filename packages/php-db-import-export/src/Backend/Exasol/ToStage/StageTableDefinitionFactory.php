<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Exasol\ToStage;

use Keboola\Datatype\Definition\Exasol;
use Keboola\Db\ImportExport\Backend\Exasol\Helper\BackendHelper;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Exasol\ExasolColumn;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

final class StageTableDefinitionFactory
{
    /**
     * @param string[] $sourceColumnsNames
     */
    public static function createStagingTableDefinition(
        TableDefinitionInterface $destination,
        array $sourceColumnsNames
    ): ExasolTableDefinition {
        /** @var ExasolTableDefinition $destination */
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
            $destination->getPrimaryKeysNames()
        );
    }

    private static function createNvarcharColumn(string $columnName): ExasolColumn
    {
        return new ExasolColumn(
            $columnName,
            new Exasol(
                Exasol::TYPE_NVARCHAR,
                [
                    'length' => Exasol::MAX_VARCHAR_LENGTH,
                    'nullable' => true, // set all columns to be nullable
                ]
            )
        );
    }

    /**
     * @param ExasolTableDefinition $destination
     * @param string[] $pkNames
     * @return ExasolTableDefinition
     */
    public static function createDedupTableDefinition(
        ExasolTableDefinition $destination,
        array $pkNames
    ): ExasolTableDefinition {
        // ensure that PK on dedup table are not null
        $dedupTableColumns = [];
        /** @var ExasolColumn $definition */
        foreach ($destination->getColumnsDefinitions() as $definition) {
            if (in_array($definition->getColumnName(), $pkNames)) {
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

        return new ExasolTableDefinition(
            $destination->getSchemaName(),
            BackendHelper::generateTempDedupTableName(),
            true,
            new ColumnCollection($dedupTableColumns),
            $pkNames
        );
    }
}
