<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Keboola\TableBackendUtils\Column\ColumnInterface;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\QueryBuilderException;
use Keboola\TableBackendUtils\TableBackendUtilException;

class SynapseTableQueryBuilder implements TableQueryBuilderInterface
{
    public const MAX_TABLE_COLUMNS = 1024;

    /** @var SQLServer2012Platform|AbstractPlatform */
    private $platform;

    public function __construct(Connection $connection)
    {
        $this->platform = $connection->getDatabasePlatform();
    }

    public function getCreateTempTableCommand(
        string $schemaName,
        string $tableName,
        array $columns
    ): string {
        $this->assertTemporaryTable($tableName);
        $this->assertTableColumnsCount($columns);

        $columnsSql = array_map(function (SynapseColumn $column) {
            return sprintf(
                '%s %s',
                $this->platform->quoteSingleIdentifier($column->getColumnName()),
                $column->getColumnDefinition()->getSQLDefinition()
            );
        }, $columns);
        return sprintf(
            'CREATE TABLE %s.%s (%s) WITH (HEAP, LOCATION = USER_DB)',
            $this->platform->quoteSingleIdentifier($schemaName),
            $this->platform->quoteSingleIdentifier($tableName),
            implode(', ', $columnsSql)
        );
    }

    private function assertTemporaryTable(string $tableName): void
    {
        if (strpos($tableName, '#') !== 0) {
            throw new TableBackendUtilException(sprintf(
                'Staging table must start with "#" table name "%s" supplied',
                $tableName
            ));
        }
    }

    public function getCreateTableCommand(
        string $schemaName,
        string $tableName,
        array $columns,
        array $primaryKeys = []
    ): string {
        $this->assertTableColumnsCount($columns);

        $columnsSql = [];
        foreach ($columns as $column) {
            if ($column->getColumnName() === self::TIMESTAMP_COLUMN_NAME) {
                $columnsSql[] = sprintf('[%s] datetime2', self::TIMESTAMP_COLUMN_NAME);
                continue;
            }
            $columnsSql[] = sprintf(
                '%s %s',
                $this->platform->quoteSingleIdentifier($column->getColumnName()),
                $column->getColumnDefinition()->getSQLDefinition()
            );
        }

        $primaryKeySql = '';
        if (!empty($primaryKeys)) {
            $quotedPrimaryKeys = array_map(function ($columnName) {
                return $this->platform->quoteSingleIdentifier($columnName);
            }, $primaryKeys);
            $primaryKeySql = sprintf(
                ', PRIMARY KEY NONCLUSTERED(%s) NOT ENFORCED',
                implode(',', $quotedPrimaryKeys)
            );
        }

        return sprintf(
            'CREATE TABLE %s.%s (%s%s)',
            $this->platform->quoteSingleIdentifier($schemaName),
            $this->platform->quoteSingleIdentifier($tableName),
            implode(', ', $columnsSql),
            $primaryKeySql
        );
    }

    public function getDropTableCommand(
        string $schemaName,
        string $tableName
    ): string {
        return sprintf(
            'DROP TABLE %s.%s',
            $this->platform->quoteSingleIdentifier($schemaName),
            $this->platform->quoteSingleIdentifier($tableName)
        );
    }

    public function getRenameTableCommand(
        string $schemaName,
        string $sourceTableName,
        string $newTableName
    ): string {
        return sprintf(
            'RENAME OBJECT %s.%s TO %s',
            $this->platform->quoteSingleIdentifier($schemaName),
            $this->platform->quoteSingleIdentifier($sourceTableName),
            $this->platform->quoteSingleIdentifier($newTableName)
        );
    }

    public function getTruncateTableCommand(
        string $schemaName,
        string $tableName
    ): string {
        return sprintf(
            'TRUNCATE TABLE %s.%s',
            $this->platform->quoteSingleIdentifier($schemaName),
            $this->platform->quoteSingleIdentifier($tableName)
        );
    }

    /**
     * @param ColumnInterface[] $columns
     */
    private function assertTableColumnsCount(array $columns): void
    {
        if (count($columns) > self::MAX_TABLE_COLUMNS) {
            throw new QueryBuilderException(
                sprintf('Too many columns. Maximum is %s columns.', self::MAX_TABLE_COLUMNS),
                QueryBuilderException::STRING_CODE_TO_MANY_COLUMNS
            );
        }
    }
}
