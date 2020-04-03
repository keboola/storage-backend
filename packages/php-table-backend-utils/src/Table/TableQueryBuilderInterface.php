<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table;

use Keboola\TableBackendUtils\Column\ColumnIterator;

interface TableQueryBuilderInterface
{
    public const TIMESTAMP_COLUMN_NAME = '_timestamp';

    public function getCreateTempTableCommand(
        string $schemaName,
        string $tableName,
        ColumnIterator $columns
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
        ColumnIterator $columns,
        array $primaryKeys = []
    ): string;
}
