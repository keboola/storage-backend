<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table;

use Keboola\TableBackendUtils\Column\ColumnCollection;

interface TableQueryBuilderInterface
{
    public const CREATE_TABLE_WITH_PRIMARY_KEYS = true;
    public const CREATE_TABLE_WITHOUT_PRIMARY_KEYS = false;

    public function getCreateTempTableCommand(
        string $schemaName,
        string $tableName,
        ColumnCollection $columns,
    ): string;

    public function getDropTableCommand(string $schemaName, string $tableName): string;

    public function getRenameTableCommand(string $schemaName, string $sourceTableName, string $newTableName): string;

    public function getTruncateTableCommand(string $schemaName, string $tableName): string;

    /**
     * @param string[] $primaryKeys
     */
    public function getCreateTableCommand(
        string $schemaName,
        string $tableName,
        ColumnCollection $columns,
        array $primaryKeys = [],
    ): string;

    public function getCreateTableCommandFromDefinition(
        TableDefinitionInterface $definition,
        bool $definePrimaryKeys = self::CREATE_TABLE_WITHOUT_PRIMARY_KEYS,
    ): string;
}
