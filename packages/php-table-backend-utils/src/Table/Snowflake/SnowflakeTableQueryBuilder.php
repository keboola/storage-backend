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
    private const CANNOT_CHANGE_DEFAULT_VALUE = 'cannotChangeDefaultValue';
    private const CANNOT_CHANGE_SCALE = 'cannotChangeScale';
    private const CANNOT_DECREASE_LENGTH = 'cannotDecreaseLength';
    private const CANNOT_DECREASE_PRECISION = 'cannotDecreasePrecision';
    private const CANNOT_INTRODUCE_COMPLEX_LENGTH = 'cannotIntroduceComplexLength';
    private const CANNOT_REDUCE_COMPLEX_LENGTH = 'cannotReduceComplexLength';
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
                $columnDefinition->getSQLDefinition(),
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
            $columnsSql,
        );
    }

    public function getDropTableCommand(string $schemaName, string $tableName): string
    {
        return sprintf(
            'DROP TABLE %s.%s',
            SnowflakeQuote::quoteSingleIdentifier($schemaName),
            SnowflakeQuote::quoteSingleIdentifier($tableName),
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
            SnowflakeQuote::quoteSingleIdentifier($newTableName),
        );
    }

    public function getTruncateTableCommand(string $schemaName, string $tableName): string
    {
        return sprintf(
            'TRUNCATE TABLE %s.%s',
            SnowflakeQuote::quoteSingleIdentifier($schemaName),
            SnowflakeQuote::quoteSingleIdentifier($tableName),
        );
    }

    /**
     * @inheritDoc
     */
    public function getCreateTableCommand(
        string $schemaName,
        string $tableName,
        ColumnCollection $columns,
        array $primaryKeys = [],
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

            $columnsSqlDefinitions[] = sprintf(
                '%s %s',
                SnowflakeQuote::quoteSingleIdentifier($columnName),
                $columnDefinition->getSQLDefinition(),
            );
        }

        // check that all PKs are valid columns
        $pksNotPresentInColumns = array_diff($primaryKeys, $columnNames);
        if ($pksNotPresentInColumns !== []) {
            throw new QueryBuilderException(
                sprintf(
                    'Trying to set %s as PKs but not present in columns',
                    implode(',', $pksNotPresentInColumns),
                ),
                self::INVALID_PKS_FOR_TABLE,
            );
        }

        if ($primaryKeys !== []) {
            $columnsSqlDefinitions[] =
                sprintf(
                    'PRIMARY KEY (%s)',
                    implode(',', array_map(
                        static fn($item) => SnowflakeQuote::quoteSingleIdentifier($item),
                        $primaryKeys,
                    )),
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
            $columnsSql,
        );
    }

    public function getCreateTableCommandFromDefinition(
        TableDefinitionInterface $definition,
        bool $definePrimaryKeys = self::CREATE_TABLE_WITHOUT_PRIMARY_KEYS,
    ): string {
        assert($definition instanceof SnowflakeTableDefinition);
        if ($definition->isTemporary()) {
            return $this->getCreateTempTableCommand(
                $definition->getSchemaName(),
                $definition->getTableName(),
                $definition->getColumnsDefinitions(),
            );
        }

        return $this->getCreateTableCommand(
            $definition->getSchemaName(),
            $definition->getTableName(),
            $definition->getColumnsDefinitions(),
            $definePrimaryKeys === self::CREATE_TABLE_WITH_PRIMARY_KEYS
                ? $definition->getPrimaryKeysNames()
                : [],
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
                    $tableName,
                ),
                self::INVALID_TABLE_NAME,
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
                    $tableName,
                ),
                self::INVALID_TABLE_NAME,
            );
        }

        if (ctype_print($tableName) === false) {
            throw new QueryBuilderException(
                sprintf(
                    'Invalid table name %s: Name can contain only printable characters.',
                    $tableName,
                ),
                self::INVALID_TABLE_NAME,
            );
        }
    }

    public static function buildTempTableName(string $realTableName): string
    {
        return self::TEMP_TABLE_PREFIX . $realTableName;
    }

    public function getUpdateColumnFromDefinitionQuery(
        Snowflake $existingColumnDefinition,
        Snowflake $desiredColumnDefinition,
        string $schemaName,
        string $tableName,
        string $columnName,
    ): string {
        $sql = sprintf(
            'ALTER TABLE %s.%s MODIFY ',
            SnowflakeQuote::quoteSingleIdentifier($schemaName),
            SnowflakeQuote::quoteSingleIdentifier($tableName),
        );
        $sqlParts = [];
        // allowed from https://docs.snowflake.com/en/sql-reference/sql/alter-table-column

        // drop default
        if ($existingColumnDefinition->getDefault() !== null
            && $desiredColumnDefinition->getDefault() === null) {
            $sqlParts[] = 'DROP DEFAULT';
        } elseif ($existingColumnDefinition->getDefault() !== $desiredColumnDefinition->getDefault()) {
            throw new QueryBuilderException(
                sprintf(
                    'Cannot change default value of column "%s" from "%s" to "%s"',
                    $columnName,
                    $existingColumnDefinition->getDefault(),
                    $desiredColumnDefinition->getDefault(),
                ),
                self::CANNOT_CHANGE_DEFAULT_VALUE,
            );
        }

        if ($existingColumnDefinition->isNullable() !== $desiredColumnDefinition->isNullable()) {
            $sqlParts[] = $desiredColumnDefinition->isNullable() ? 'DROP NOT NULL' : 'SET NOT NULL';
        }

        $notSameLength = $existingColumnDefinition->getLength() !== $desiredColumnDefinition->getLength();
        $isNewLengthBigger = $existingColumnDefinition->getLength() < $desiredColumnDefinition->getLength();

        // increase precision
        if ($existingColumnDefinition->isTypeWithComplexLength() && $notSameLength) {
            if (!$desiredColumnDefinition->isTypeWithComplexLength()) {
                throw new QueryBuilderException(
                    sprintf(
                        'Cannot reduce column "%s" with complex length "%s" to simple length "%s"',
                        $columnName,
                        $existingColumnDefinition->getLength(),
                        $desiredColumnDefinition->getLength(),
                    ),
                    self::CANNOT_REDUCE_COMPLEX_LENGTH,
                );
            }
            [
                'numeric_precision' => $existingPrecision,
                'numeric_scale' => $existingScale,
            ] = $existingColumnDefinition->getArrayFromLength();
            [
                'numeric_precision' => $desiredPrecision,
                'numeric_scale' => $desiredScale,
            ] = $desiredColumnDefinition->getArrayFromLength();

            if ($existingScale !== $desiredScale) {
                throw new QueryBuilderException(
                    sprintf(
                        'Cannot change scale of a column "%s" from "%s" to "%s"',
                        $columnName,
                        $existingScale,
                        $desiredScale,
                    ),
                    self::CANNOT_CHANGE_SCALE,
                );
            }

            if ($existingPrecision < $desiredPrecision) {
                $sqlParts[] = sprintf(
                    'SET DATA TYPE %s(%s, %s)',
                    $desiredColumnDefinition->getType(),
                    $desiredPrecision,
                    $desiredScale,
                );
            } else {
                throw new QueryBuilderException(
                    sprintf(
                        'Cannot decrease precision of column "%s" from "%s" to "%s"',
                        $columnName,
                        $existingPrecision,
                        $desiredPrecision,
                    ),
                    self::CANNOT_DECREASE_PRECISION,
                );
            }
        } elseif ($notSameLength && $isNewLengthBigger) {
            if ($desiredColumnDefinition->isTypeWithComplexLength()) {
                throw new QueryBuilderException(
                    sprintf(
                        'Cannot convert column "%s" from simple length "%s" to complex length "%s"',
                        $columnName,
                        $existingColumnDefinition->getLength(),
                        $desiredColumnDefinition->getLength(),
                    ),
                    self::CANNOT_INTRODUCE_COMPLEX_LENGTH,
                );
            }
            // increase length
            $sqlParts[] = sprintf(
                'SET DATA TYPE %s(%s)',
                $desiredColumnDefinition->getType(),
                $desiredColumnDefinition->getLength(),
            );
        } elseif ($notSameLength) {
            throw new QueryBuilderException(
                sprintf(
                    'Cannot decrease length of column "%s" from "%s" to "%s"',
                    $columnName,
                    $existingColumnDefinition->getLength(),
                    $desiredColumnDefinition->getLength(),
                ),
                self::CANNOT_DECREASE_LENGTH,
            );
        }

        $partsWithColumnPrefix = array_map(function (string $part) use ($columnName) {
            return sprintf(
                'COLUMN %s %s',
                SnowflakeQuote::quoteSingleIdentifier($columnName),
                $part,
            );
        }, $sqlParts);
        return $sql . implode(', ', $partsWithColumnPrefix);
    }
}
