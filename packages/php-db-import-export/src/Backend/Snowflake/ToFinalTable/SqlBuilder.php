<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable;

use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\SourceDestinationColumnMap;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;

class SqlBuilder
{
    private const AUTO_CASTING_TYPES = [
        Snowflake::TYPE_VARIANT,
        Snowflake::TYPE_OBJECT,
        Snowflake::TYPE_ARRAY,
        Snowflake::TYPE_VECTOR,
    ];
    public const SRC_ALIAS = 'src';

    public function getBeginTransaction(): string
    {
        return 'BEGIN TRANSACTION';
    }

    public function getCommitTransaction(): string
    {
        return 'COMMIT';
    }

    /**
     * @param string[] $primaryKeys
     */
    public function getDedupCommand(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $deduplicationTableDefinition,
        array $primaryKeys,
    ): string {
        if (empty($primaryKeys)) {
            return '';
        }

        $pkSql = $this->getColumnsString(
            $primaryKeys,
            ',',
        );

        $stage = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName()),
        );

        $depudeSql = sprintf(
            'SELECT %s FROM ('
            . 'SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) AS "_row_number_" '
            . 'FROM %s'
            . ') AS a '
            . 'WHERE a."_row_number_" = 1',
            $this->getColumnsString($deduplicationTableDefinition->getColumnsNames(), ',', 'a'),
            $this->getColumnsString($deduplicationTableDefinition->getColumnsNames(), ', '),
            $pkSql,
            $pkSql,
            $stage,
        );

        $deduplication = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($deduplicationTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($deduplicationTableDefinition->getTableName()),
        );

        return sprintf(
            'INSERT INTO %s (%s) %s',
            $deduplication,
            $this->getColumnsString($deduplicationTableDefinition->getColumnsNames()),
            $depudeSql,
        );
    }

    /**
     * @param string[] $columns
     */
    public function getColumnsString(
        array $columns,
        string $delimiter = ', ',
        ?string $tableAlias = null,
    ): string {
        return implode($delimiter, array_map(static function ($columns) use (
            $tableAlias,
        ) {
            $alias = $tableAlias === null ? '' : $tableAlias . '.';
            return $alias . SnowflakeQuote::quoteSingleIdentifier($columns);
        }, $columns));
    }

    public function getDeleteOldItemsCommand(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition,
        SnowflakeImportOptions $importOptions,
    ): string {
        $stagingTable = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName()),
        );

        $destinationTable = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getTableName()),
        );

        return sprintf(
            'DELETE FROM %s "src" USING %s AS "dest" WHERE %s',
            $stagingTable,
            $destinationTable,
            $this->getPrimayKeyWhereConditions(
                $destinationTableDefinition->getPrimaryKeysNames(),
                $importOptions,
            ),
        );
    }

    /**
     * @param string[] $primaryKeys
     */
    private function getPrimayKeyWhereConditions(
        array $primaryKeys,
        SnowflakeImportOptions $importOptions,
    ): string {
        $pkWhereSql = array_map(function (string $col) use ($importOptions) {
            $str = '"dest".%s = COALESCE("src".%s, \'\')';
            if (!$importOptions->isNullManipulationEnabled()) {
                $str = '"dest".%s = "src".%s';
            }
            return sprintf(
                $str,
                QuoteHelper::quoteIdentifier($col),
                QuoteHelper::quoteIdentifier($col),
            );
        }, $primaryKeys);

        return implode(' AND ', $pkWhereSql) . ' ';
    }

    public function getDropTableIfExistsCommand(
        string $schema,
        string $tableName,
    ): string {
        return sprintf(
            'DROP TABLE IF EXISTS %s.%s',
            SnowflakeQuote::quoteSingleIdentifier($schema),
            SnowflakeQuote::quoteSingleIdentifier($tableName),
        );
    }

    public function getInsertAllIntoTargetTableCommand(
        SnowflakeTableDefinition $sourceTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition,
        SnowflakeImportOptions $importOptions,
        string $timestamp,
    ): string {
        $columnMap = SourceDestinationColumnMap::createForTables(
            $sourceTableDefinition,
            $destinationTableDefinition,
            $importOptions->ignoreColumns(),
            SourceDestinationColumnMap::MODE_MAP_BY_NAME,
        );
        $destinationTable = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getTableName()),
        );

        $insColumns = $sourceTableDefinition->getColumnsNames();
        $useTimestamp = !in_array(ToStageImporterInterface::TIMESTAMP_COLUMN_NAME, $insColumns, true)
            && $importOptions->useTimestamp();

        if ($useTimestamp) {
            $insColumns = array_merge(
                $sourceTableDefinition->getColumnsNames(),
                [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
            );
        }

        $columnsSetSql = [];

        /** @var SnowflakeColumn $sourceColumn */
        foreach ($sourceTableDefinition->getColumnsDefinitions() as $sourceColumn) {
            // output mapping same tables are required do not convert nulls to empty strings
            if (!$importOptions->isNullManipulationEnabled()) {
                $destinationColumn = $columnMap->getDestination($sourceColumn);
                $type = $destinationColumn->getColumnDefinition()->getType();
                $useAutoCast = in_array($type, self::AUTO_CASTING_TYPES, true);
                $isSameType = $type === $sourceColumn->getColumnDefinition()->getType();
                if ($useAutoCast && !$isSameType) {
                    if ($type === Snowflake::TYPE_OBJECT) {
                        // object can't be casted from string but can be casted from variant
                        $columnsSetSql[] = sprintf(
                            'CAST(TO_VARIANT(%s) AS %s) AS %s',
                            SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                            $destinationColumn->getColumnDefinition()->getSQLDefinition(),
                            SnowflakeQuote::quoteSingleIdentifier($destinationColumn->getColumnName()),
                        );
                        continue;
                    }
                    if ($type === Snowflake::TYPE_ARRAY) {
                        $columnsSetSql[] = sprintf(
                            'CAST(PARSE_JSON(%s) AS %s) AS %s',
                            SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                            $destinationColumn->getColumnDefinition()->getSQLDefinition(),
                            SnowflakeQuote::quoteSingleIdentifier($destinationColumn->getColumnName()),
                        );
                        continue;
                    }
                    if ($type === Snowflake::TYPE_VECTOR) {
                        $columnsSetSql[] = sprintf(
                            'CAST(PARSE_JSON(%s) AS ARRAY)::%s AS %s',
                            SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                            $destinationColumn->getColumnDefinition()->getSQLDefinition(),
                            SnowflakeQuote::quoteSingleIdentifier($destinationColumn->getColumnName()),
                        );
                        continue;
                    }
                    $columnsSetSql[] = sprintf(
                        'CAST(%s AS %s) AS %s',
                        SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                        $destinationColumn->getColumnDefinition()->getSQLDefinition(),
                        SnowflakeQuote::quoteSingleIdentifier($destinationColumn->getColumnName()),
                    );
                    continue;
                }
                $columnsSetSql[] = SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName());
                continue;
            }

            // Input mapping convert empty values to null
            // empty strings '' are converted to null values
            if (in_array($sourceColumn->getColumnName(), $importOptions->getConvertEmptyValuesToNull(), true)) {
                // use nullif only for string base type
                if ($sourceColumn->getColumnDefinition()->getBasetype() === BaseType::STRING) {
                    $columnsSetSql[] = sprintf(
                        'IFF(%s = \'\', NULL, %s)',
                        SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                        SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                    );
                    continue;
                }
                // if tables is not typed column could be other than string in this case we skip conversion
                $columnsSetSql[] = SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName());
                continue;
            }

            // for string base type convert null values to empty string ''
            //phpcs:ignore
            if (!$importOptions->usingUserDefinedTypes() && $sourceColumn->getColumnDefinition()->getBasetype() === BaseType::STRING) {
                $columnsSetSql[] = sprintf(
                    'COALESCE(%s, \'\') AS %s',
                    SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                    SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                );
                continue;
            }
            // on columns other than string dont use COALESCE
            // this will fail if the column is not null, but this is expected
            $columnsSetSql[] = SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName());
        }

        if ($useTimestamp) {
            $columnsSetSql[] = SnowflakeQuote::quote($timestamp);
        }

        return sprintf(
            'INSERT INTO %s (%s) (SELECT %s FROM %s.%s AS %s)',
            $destinationTable,
            $this->getColumnsString($insColumns),
            implode(',', $columnsSetSql),
            SnowflakeQuote::quoteSingleIdentifier($sourceTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($sourceTableDefinition->getTableName()),
            SnowflakeQuote::quoteSingleIdentifier(self::SRC_ALIAS),
        );
    }

    public function getTruncateTable(
        string $schema,
        string $tableName,
    ): string {
        return sprintf(
            'TRUNCATE TABLE %s.%s',
            SnowflakeQuote::quoteSingleIdentifier($schema),
            SnowflakeQuote::quoteSingleIdentifier($tableName),
        );
    }

    /**
     * Generates a CREATE TABLE AS SELECT (CTAS) command to create the destination table from the staging table.
     * Adds a _timestamp column with the current timestamp value.
     */
    public function getCTASInsertAllIntoTargetTableCommand(
        SnowflakeTableDefinition $sourceTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition,
    ): string {
        $timestampColumn = '';
        // todo check also timestamp data type
        if (!in_array(ToStageImporterInterface::TIMESTAMP_COLUMN_NAME, $sourceTableDefinition->getColumnsNames())) {
            $timestampColumn = sprintf(
                'current_timestamp() AS %s',
                SnowflakeQuote::quoteSingleIdentifier(ToStageImporterInterface::TIMESTAMP_COLUMN_NAME),
            );
        }
        // Build the source table reference
        $sourceTable = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($sourceTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($sourceTableDefinition->getTableName()),
        );

        // Build the destination table reference
        $destinationTable = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getTableName()),
        );

        // Create the CTAS command
        return sprintf(
            'CREATE OR REPLACE TABLE %s AS SELECT *,%s FROM %s',
            $destinationTable,
            $timestampColumn,
            $sourceTable,
        );
    }

    public function getUpdateWithPkCommand(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $destinationDefinition,
        SnowflakeImportOptions $importOptions,
        string $timestamp,
    ): string {
        $columnMap = SourceDestinationColumnMap::createForTables(
            $stagingTableDefinition,
            $destinationDefinition,
            $importOptions->ignoreColumns(),
            SourceDestinationColumnMap::MODE_MAP_BY_NAME,
        );
        $columnsSet = [];

        foreach ($stagingTableDefinition->getColumnsDefinitions() as $sourceColumn) {
            if (!$importOptions->isNullManipulationEnabled()) {
                $destinationColumn = $columnMap->getDestination($sourceColumn);
                $type = $destinationColumn->getColumnDefinition()->getType();
                $useAutoCast = in_array($type, self::AUTO_CASTING_TYPES, true);
                $isSameType = $type === $sourceColumn->getColumnDefinition()->getType();
                if ($useAutoCast && !$isSameType) {
                    if ($type === Snowflake::TYPE_OBJECT) {
                        // object can't be casted from string but can be casted from variant
                        $columnsSet[] = sprintf(
                            '%s = CAST(TO_VARIANT("src".%s) AS %s)',
                            SnowflakeQuote::quoteSingleIdentifier($destinationColumn->getColumnName()),
                            SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                            $destinationColumn->getColumnDefinition()->getSQLDefinition(),
                        );
                        continue;
                    }
                    if ($type === Snowflake::TYPE_ARRAY) {
                        $columnsSet[] = sprintf(
                            '%s = CAST(PARSE_JSON("src".%s) AS %s)',
                            SnowflakeQuote::quoteSingleIdentifier($destinationColumn->getColumnName()),
                            SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                            $destinationColumn->getColumnDefinition()->getSQLDefinition(),
                        );
                        continue;
                    }
                    if ($type === Snowflake::TYPE_VECTOR) {
                        $columnsSet[] = sprintf(
                            '%s = CAST(PARSE_JSON("src".%s) AS ARRAY)::%s',
                            SnowflakeQuote::quoteSingleIdentifier($destinationColumn->getColumnName()),
                            SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                            $destinationColumn->getColumnDefinition()->getSQLDefinition(),
                        );
                        continue;
                    }
                    $columnsSet[] = sprintf(
                        '%s = CAST("src".%s AS %s)',
                        SnowflakeQuote::quoteSingleIdentifier($destinationColumn->getColumnName()),
                        SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                        $destinationColumn->getColumnDefinition()->getSQLDefinition(),
                    );
                    continue;
                }

                $columnsSet[] = sprintf(
                    '%s = "src".%s',
                    SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                    SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                );
                continue;
            }
            if (in_array($sourceColumn->getColumnName(), $importOptions->getConvertEmptyValuesToNull(), true)) {
                $columnsSet[] = sprintf(
                    '%s = IFF("src".%s = \'\', NULL, "src".%s)',
                    SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                    SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                    SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                );
            } else {
                $columnsSet[] = sprintf(
                    '%s = COALESCE("src".%s, \'\')',
                    SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                    SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                );
            }
        }

        if ($importOptions->useTimestamp()) {
            $columnsSet[] = sprintf(
                '%s = \'%s\'',
                SnowflakeQuote::quoteSingleIdentifier(ToStageImporterInterface::TIMESTAMP_COLUMN_NAME),
                $timestamp,
            );
        }

        $columnsComparisonSql = [];
        if ($importOptions->isNullManipulationEnabled()) {
            // update only changed rows - mysql TIMESTAMP ON UPDATE behaviour simulation
            $columnsComparisonSql = array_map(
                static function ($columnName) {
                    return sprintf(
                        'COALESCE(TO_VARCHAR("dest".%s), \'\') != COALESCE("src".%s, \'\')',
                        SnowflakeQuote::quoteSingleIdentifier($columnName),
                        SnowflakeQuote::quoteSingleIdentifier($columnName),
                    );
                },
                $stagingTableDefinition->getColumnsNames(),
            );
        } else {
            foreach ($stagingTableDefinition->getColumnsDefinitions() as $sourceColumn) {
                $destinationColumn = $columnMap->getDestination($sourceColumn);
                if (in_array($destinationColumn->getColumnDefinition()->getType(), [
                    Snowflake::TYPE_GEOGRAPHY,
                    Snowflake::TYPE_GEOMETRY,
                ], true)) {
                    $columnsComparisonSql[] = sprintf(
                        'ST_ASEWKT("dest".%s) IS DISTINCT FROM ST_ASEWKT("src".%s)',
                        SnowflakeQuote::quoteSingleIdentifier($destinationColumn->getColumnName()),
                        SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                    );
                } else {
                    $columnsComparisonSql[] = sprintf(
                        '"dest".%s IS DISTINCT FROM "src".%s',
                        SnowflakeQuote::quoteSingleIdentifier($destinationColumn->getColumnName()),
                        SnowflakeQuote::quoteSingleIdentifier($sourceColumn->getColumnName()),
                    );
                }
            }
        }

        $dest = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($destinationDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($destinationDefinition->getTableName()),
        );

        if ($columnsComparisonSql === []) {
            return sprintf(
                'UPDATE %s AS "dest" SET %s FROM %s.%s AS "src" WHERE %s',
                $dest,
                implode(', ', $columnsSet),
                QuoteHelper::quoteIdentifier($stagingTableDefinition->getSchemaName()),
                QuoteHelper::quoteIdentifier($stagingTableDefinition->getTableName()),
                $this->getPrimayKeyWhereConditions($destinationDefinition->getPrimaryKeysNames(), $importOptions),
            );
        }

        return sprintf(
            'UPDATE %s AS "dest" SET %s FROM %s.%s AS "src" WHERE %s AND (%s)',
            $dest,
            implode(', ', $columnsSet),
            QuoteHelper::quoteIdentifier($stagingTableDefinition->getSchemaName()),
            QuoteHelper::quoteIdentifier($stagingTableDefinition->getTableName()),
            $this->getPrimayKeyWhereConditions($destinationDefinition->getPrimaryKeysNames(), $importOptions),
            implode(' OR ', $columnsComparisonSql),
        );
    }
}
