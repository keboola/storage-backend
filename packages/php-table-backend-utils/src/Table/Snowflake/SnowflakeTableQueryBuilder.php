<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Snowflake;

use Keboola\Datatype\Definition\Snowflake;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\QueryBuilderException;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\TableQueryBuilderInterface;

class SnowflakeTableQueryBuilder implements TableQueryBuilderInterface
{
    private const INVALID_PKS_FOR_TABLE = 'invalidPKs';
    private const INVALID_TABLE_NAME = 'invalidTableName';
    public const TEMP_TABLE_PREFIX = '__temp_';

    public function getCreateTempTableCommand(string $schemaName, string $tableName, ColumnCollection $columns): string
    {
        $this->assertStagingTableName($tableName);

        $columnsSqlDefinitions = [];
        /** @var SnowflakeColumn $column */
        foreach ($columns->getIterator() as $column) {
            /** @var Snowflake $columnDefinition */
            $columnDefinition = $column->getColumnDefinition();
            $columnsSqlDefinitions[] = sprintf(
                '%s %s',
                SnowflakeQuote::quoteSingleIdentifier($column->getColumnName()),
                $columnDefinition->getSQLDefinition()
            );
        }

        $columnsSql = implode(",\n", $columnsSqlDefinitions);

        return sprintf(
            'CREATE TEMPORARY TABLE %s.%s
(
%s
);',
            SnowflakeQuote::quoteSingleIdentifier($schemaName),
            SnowflakeQuote::quoteSingleIdentifier($tableName),
            $columnsSql
        );
    }

    public function getDropTableCommand(string $schemaName, string $tableName): string
    {
        return sprintf(
            'DROP TABLE %s.%s',
            SnowflakeQuote::quoteSingleIdentifier($schemaName),
            SnowflakeQuote::quoteSingleIdentifier($tableName)
        );
    }

    public function getRenameTableCommand(string $schemaName, string $sourceTableName, string $newTableName): string
    {
        $this->assertTableName($newTableName);

        $quotedDbName = SnowflakeQuote::quoteSingleIdentifier($schemaName);
        return sprintf(
            'ALTER TABLE %s.%s RENAME TO %s.%s',
            $quotedDbName,
            SnowflakeQuote::quoteSingleIdentifier($sourceTableName),
            $quotedDbName,
            SnowflakeQuote::quoteSingleIdentifier($newTableName)
        );
    }

    public function getTruncateTableCommand(string $schemaName, string $tableName): string
    {
        return sprintf(
            'TRUNCATE TABLE %s.%s',
            SnowflakeQuote::quoteSingleIdentifier($schemaName),
            SnowflakeQuote::quoteSingleIdentifier($tableName)
        );
    }

    /**
     * @inheritDoc
     */
    public function getCreateTableCommand(
        string $schemaName,
        string $tableName,
        ColumnCollection $columns,
        array $primaryKeys = []
    ): string {
        $this->assertTableName($tableName);

        $columnsSqlDefinitions = [];
        $columnNames = [];
        /** @var SnowflakeColumn $column */
        foreach ($columns->getIterator() as $column) {
            $columnName = $column->getColumnName();
            $columnNames[] = $columnName;
            /** @var Snowflake $columnDefinition */
            $columnDefinition = $column->getColumnDefinition();

            // add PK on nullable column is legal, but SNFLK will force it to non-nullable. So rather check it first
            if ($primaryKeys && in_array($columnName, $primaryKeys, true) && $columnDefinition->isNullable()) {
                throw new QueryBuilderException(
                    sprintf('Trying to set PK on column %s but this column is nullable', $columnName),
                    self::INVALID_PKS_FOR_TABLE
                );
            }

            $columnsSqlDefinitions[] = sprintf(
                '%s %s',
                SnowflakeQuote::quoteSingleIdentifier($columnName),
                $columnDefinition->getSQLDefinition()
            );
        }

        // check that all PKs are valid columns
        $pksNotPresentInColumns = array_diff($primaryKeys, $columnNames);
        if ($pksNotPresentInColumns !== []) {
            throw new QueryBuilderException(
                sprintf(
                    'Trying to set %s as PKs but not present in columns',
                    implode(',', $pksNotPresentInColumns)
                ),
                self::INVALID_PKS_FOR_TABLE
            );
        }

        if ($primaryKeys !== []) {
            $columnsSqlDefinitions[] =
                sprintf(
                    'PRIMARY KEY (%s)',
                    implode(',', array_map(
                        static fn($item) => SnowflakeQuote::quoteSingleIdentifier($item),
                        $primaryKeys
                    ))
                );
        }

        $columnsSql = implode(",\n", $columnsSqlDefinitions);

        // brackets on single rows because in order to have much more beautiful query at the end
        return sprintf(
            'CREATE TABLE %s.%s
(
%s
);',
            SnowflakeQuote::quoteSingleIdentifier($schemaName),
            SnowflakeQuote::quoteSingleIdentifier($tableName),
            $columnsSql
        );
    }

    /**
     * @param SnowflakeTableDefinition $definition
     */
    public function getCreateTableCommandFromDefinition(
        TableDefinitionInterface $definition,
        bool $definePrimaryKeys = self::CREATE_TABLE_WITHOUT_PRIMARY_KEYS
    ): string {
        assert($definition instanceof SnowflakeTableDefinition);
        if ($definition->isTemporary()) {
            return $this->getCreateTempTableCommand(
                $definition->getSchemaName(),
                $definition->getTableName(),
                $definition->getColumnsDefinitions()
            );
        }

        return $this->getCreateTableCommand(
            $definition->getSchemaName(),
            $definition->getTableName(),
            $definition->getColumnsDefinitions(),
            $definePrimaryKeys === self::CREATE_TABLE_WITH_PRIMARY_KEYS
                ? $definition->getPrimaryKeysNames()
                : []
        );
    }

    /**
     * checks that table name has __temp_ prefix which is required for temp tables
     */
    private function assertStagingTableName(string $tableName): void
    {
        $this->assertTableName($tableName);
        if ($tableName === self::TEMP_TABLE_PREFIX || strpos($tableName, self::TEMP_TABLE_PREFIX) !== 0) {
            throw new QueryBuilderException(
                sprintf(
                    'Invalid table name %s: Table must start with __temp_ prefix',
                    $tableName
                ),
                self::INVALID_TABLE_NAME
            );
        }
    }

    private function assertTableName(string $tableName): void
    {
        if (preg_match('/^[-_A-Za-z\d$]+$/', $tableName, $out) !== 1) {
            throw new QueryBuilderException(
                sprintf(
                    // phpcs:ignore
                    'Invalid table name %s: Only alphanumeric characters, dash, underscores and dollar signs are allowed.',
                    $tableName
                ),
                self::INVALID_TABLE_NAME
            );
        }

        if (ctype_print($tableName) === false) {
            throw new QueryBuilderException(
                sprintf(
                    'Invalid table name %s: Name can contain only printable characters.',
                    $tableName
                ),
                self::INVALID_TABLE_NAME
            );
        }
    }

    public static function buildTempTableName(string $realTableName): string
    {
        return self::TEMP_TABLE_PREFIX . $realTableName;
    }
}
