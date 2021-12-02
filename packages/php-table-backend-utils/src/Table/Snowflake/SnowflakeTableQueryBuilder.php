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

    public function getCreateTempTableCommand(string $schemaName, string $tableName, ColumnCollection $columns): string
    {
        // TODO: Implement getCreateTempTableCommand() method.
        throw new \Exception('method is not implemented yet');
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
        if (!$this->validateTableName($newTableName)) {
            throw new QueryBuilderException(
                sprintf(
                    'Invalid table name %s: Only alphanumeric characters, underscores and dollar signs are allowed.',
                    $newTableName
                ),
                self::INVALID_TABLE_NAME
            );
        }

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
        if (!$this->validateTableName($tableName)) {
            throw new QueryBuilderException(
                sprintf(
                    'Invalid table name %s: Only alphanumeric characters, underscores and dollar signs are allowed.',
                    $tableName
                ),
                self::INVALID_TABLE_NAME
            );
        }

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

        if ($primaryKeys) {
            $columnsSqlDefinitions[] =
                sprintf(
                    'PRIMARY KEY (%s)',
                    implode(',', array_map(static function ($item) {
                        return SnowflakeQuote::quoteSingleIdentifier($item);
                    }, $primaryKeys))
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
        return $this->getCreateTableCommand(
            $definition->getSchemaName(),
            $definition->getTableName(),
            $definition->getColumnsDefinitions(),
            $definePrimaryKeys === self::CREATE_TABLE_WITH_PRIMARY_KEYS
                ? $definition->getPrimaryKeysNames()
                : []
        );
    }

    private function validateTableName(string $tableName): bool
    {
        return (bool) preg_match('/^[A-Za-z][_A-Za-z0-9$]*$/', $tableName, $out);
    }
}
