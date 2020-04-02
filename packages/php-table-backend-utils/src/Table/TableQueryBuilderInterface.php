<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table;

use Keboola\TableBackendUtils\Column\ColumnInterface;
use Keboola\TableBackendUtils\Column\SynapseColumn;

interface TableQueryBuilderInterface
{
    public const TIMESTAMP_COLUMN_NAME = '_timestamp';

    /**
     * @param string $schemaName
     * @param string $tableName
     * @param ColumnInterface[] $columns
     * @return string
     */
    public function getCreateTempTableCommand(
        string $schemaName,
        string $tableName,
        array $columns
    ): string;

    public function getDropTableCommand(string $schemaName, string $tableName): string;

    public function getRenameTableCommand(string $schemaName, string $sourceTableName, string $newTableName): string;

    public function getTruncateTableCommand(string $schemaName, string $tableName): string;

    /**
     * @param string $schemaName
     * @param string $tableName
     * @param ColumnInterface[] $columns
     * @param string[] $primaryKeys
     * @return string
     */
    public function getCreateTableCommand(
        string $schemaName,
        string $tableName,
        array $columns,
        array $primaryKeys = []
    ): string;
}
