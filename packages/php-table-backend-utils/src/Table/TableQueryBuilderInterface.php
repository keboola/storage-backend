<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table;

use Keboola\TableBackendUtils\Column\ColumnCollection;

interface TableQueryBuilderInterface
{
    public function getCreateTempTableCommand(
        string $schemaName,
        string $tableName,
        ColumnCollection $columns
    ): string;

    public function getDropTableCommand(string $dbName, string $tableName): string;

    public function getRenameTableCommand(string $dbName, string $sourceTableName, string $newTableName): string;

    public function getTruncateTableCommand(string $schemaName, string $tableName): string;

    /**
     * @param string[] $primaryKeys
     */
    public function getCreateTableCommand(
        string $schemaName,
        string $tableName,
        ColumnCollection $columns,
        array $primaryKeys = []
    ): string;
}
