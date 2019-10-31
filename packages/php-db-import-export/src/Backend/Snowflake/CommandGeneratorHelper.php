<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Keboola\Db\ImportExport\Backend\Snowflake\Helper\ColumnsHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;

class CommandGeneratorHelper
{
    public const SLICED_FILES_CHUNK_SIZE = 1000;
    public const TIMESTAMP_COLUMN_NAME = '_timestamp';

    public static function buildBeginTransactionCommand(): string
    {
        return 'BEGIN TRANSACTION';
    }

    public static function buildCommitTransactionCommand(): string
    {
        return 'COMMIT';
    }

    public static function buildCreateStagingTableCommand(
        string $schema,
        string $tableName,
        array $columns
    ): string {
        $columnsSql = array_map(function ($column) {
            return sprintf('%s varchar', QuoteHelper::quoteIdentifier($column));
        }, $columns);
        return sprintf(
            'CREATE TEMPORARY TABLE %s.%s (%s)',
            QuoteHelper::quoteIdentifier($schema),
            QuoteHelper::quoteIdentifier($tableName),
            implode(', ', $columnsSql)
        );
    }

    public static function buildDedupCommand(
        ImportOptions $importOptions,
        array $primaryKeys,
        string $stagingTableName,
        string $tempTableName
    ): string {
        if (empty($primaryKeys)) {
            return '';
        }

        $pkSql = ColumnsHelper::getColumnsString(
            $primaryKeys,
            ','
        );

        $depudeSql = sprintf(
            'SELECT %s FROM ('
                .'SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) AS "_row_number_"'
                .'FROM %s.%s'
            .') AS a '
            .'WHERE a."_row_number_" = 1',
            ColumnsHelper::getColumnsString($importOptions->getColumns(), ',', 'a'),
            ColumnsHelper::getColumnsString($importOptions->getColumns(), ', '),
            $pkSql,
            $pkSql,
            QuoteHelper::quoteIdentifier($importOptions->getSchema()),
            QuoteHelper::quoteIdentifier($stagingTableName)
        );

        return sprintf(
            'INSERT INTO %s.%s (%s) %s',
            QuoteHelper::quoteIdentifier($importOptions->getSchema()),
            QuoteHelper::quoteIdentifier($tempTableName),
            ColumnsHelper::getColumnsString($importOptions->getColumns()),
            $depudeSql
        );
    }

    public static function buildDeleteOldItemsCommand(
        ImportOptions $importOptions,
        string $stagingTableName,
        array $primaryKeys
    ): string {
        // Delete updated rows from staging table
        return sprintf(
            'DELETE FROM %s.%s "src" USING %s AS "dest" WHERE %s',
            QuoteHelper::quoteIdentifier($importOptions->getSchema()),
            QuoteHelper::quoteIdentifier($stagingTableName),
            $importOptions->getTargetTableWithScheme(),
            self::buildPrimayKeyWhereConditions($primaryKeys)
        );
    }

    private static function buildPrimayKeyWhereConditions(
        array $primaryKeys
    ): string {
        $pkWhereSql = array_map(function (string $col) {
            return sprintf(
                '"dest".%s = COALESCE("src".%s, \'\')',
                QuoteHelper::quoteIdentifier($col),
                QuoteHelper::quoteIdentifier($col)
            );
        }, $primaryKeys);

        return implode(' AND ', $pkWhereSql) . ' ';
    }

    public static function buildDropCommand(
        string $schema,
        string $tableName
    ): string {
        return sprintf(
            'DROP TABLE %s.%s',
            QuoteHelper::quoteIdentifier($schema),
            QuoteHelper::quoteIdentifier($tableName)
        );
    }

    public static function buildInsertAllIntoTargetTableCommand(
        ImportOptions $importOptions,
        string $stagingTableName
    ): string {
        $columnsSetSqlSelect = implode(', ', array_map(function ($column) use (
            $importOptions
        ) {
            if (in_array($column, $importOptions->getConvertEmptyValuesToNull())) {
                return sprintf(
                    'IFF(%s = \'\', NULL, %s)',
                    QuoteHelper::quoteIdentifier($column),
                    QuoteHelper::quoteIdentifier($column)
                );
            }

            return sprintf(
                "COALESCE(%s, '') AS %s",
                QuoteHelper::quoteIdentifier($column),
                QuoteHelper::quoteIdentifier($column)
            );
        }, $importOptions->getColumns()));

        if (in_array(self::TIMESTAMP_COLUMN_NAME, $importOptions->getColumns())
            || $importOptions->useTimestamp() === false
        ) {
            return sprintf(
                'INSERT INTO %s.%s (%s) (SELECT %s FROM %s.%s)',
                QuoteHelper::quoteIdentifier($importOptions->getSchema()),
                QuoteHelper::quoteIdentifier($importOptions->getTableName()),
                ColumnsHelper::getColumnsString($importOptions->getColumns()),
                $columnsSetSqlSelect,
                QuoteHelper::quoteIdentifier($importOptions->getSchema()),
                QuoteHelper::quoteIdentifier($stagingTableName)
            );
        }

        return sprintf(
            'INSERT INTO %s.%s (%s, "%s") (SELECT %s, \'%s\' FROM %s.%s)',
            QuoteHelper::quoteIdentifier($importOptions->getSchema()),
            QuoteHelper::quoteIdentifier($importOptions->getTableName()),
            ColumnsHelper::getColumnsString($importOptions->getColumns()),
            self::TIMESTAMP_COLUMN_NAME,
            $columnsSetSqlSelect,
            DateTimeHelper::getNowFormatted(),
            QuoteHelper::quoteIdentifier($importOptions->getSchema()),
            QuoteHelper::quoteIdentifier($stagingTableName)
        );
    }

    public static function buildInsertFromStagingToTargetTableCommand(
        ImportOptions $importOptions,
        string $stagingTableName
    ): string {
        if ($importOptions->useTimestamp()) {
            $insColumns = array_merge($importOptions->getColumns(), [self::TIMESTAMP_COLUMN_NAME]);
        } else {
            $insColumns = $importOptions->getColumns();
        }

        $columnsSetSql = [];

        foreach ($importOptions->getColumns() as $columnName) {
            if (in_array($columnName, $importOptions->getConvertEmptyValuesToNull())) {
                $columnsSetSql[] = sprintf(
                    'IFF("src".%s = \'\', NULL, %s)',
                    QuoteHelper::quoteIdentifier($columnName),
                    QuoteHelper::quoteIdentifier($columnName)
                );
            } else {
                $columnsSetSql[] = sprintf(
                    'COALESCE("src".%s, \'\')',
                    QuoteHelper::quoteIdentifier($columnName)
                );
            }
        }

        if ($importOptions->useTimestamp()) {
            $columnsSetSql[] = sprintf('\'%s\'', DateTimeHelper::getNowFormatted());
        }

        return sprintf(
            'INSERT INTO %s.%s (%s) SELECT %s FROM %s.%s AS "src"',
            QuoteHelper::quoteIdentifier($importOptions->getSchema()),
            QuoteHelper::quoteIdentifier($importOptions->getTableName()),
            ColumnsHelper::getColumnsString($insColumns),
            implode(',', $columnsSetSql),
            QuoteHelper::quoteIdentifier($importOptions->getSchema()),
            QuoteHelper::quoteIdentifier($stagingTableName)
        );
    }

    public static function buildRenameTableCommand(
        string $schema,
        string $sourceTableName,
        string $targetTable
    ): string {
        return sprintf(
            'ALTER TABLE %s.%s RENAME TO %s.%s',
            QuoteHelper::quoteIdentifier($schema),
            QuoteHelper::quoteIdentifier($sourceTableName),
            QuoteHelper::quoteIdentifier($schema),
            QuoteHelper::quoteIdentifier($targetTable)
        );
    }

    public static function buildTruncateTableCommand(
        string $schema,
        string $tableName
    ): string {
        return sprintf(
            'TRUNCATE %s.%s',
            QuoteHelper::quoteIdentifier($schema),
            QuoteHelper::quoteIdentifier($tableName)
        );
    }

    public static function buildUpdateWithPkCommand(
        ImportOptions $importOptions,
        string $stagingTableName,
        array $primaryKeys
    ): string {
        $columnsSet = [];
        foreach ($importOptions->getColumns() as $columnName) {
            if (in_array($columnName, $importOptions->getConvertEmptyValuesToNull())) {
                $columnsSet[] = sprintf(
                    '%s = IFF("src".%s = \'\', NULL, "src".%s)',
                    QuoteHelper::quoteIdentifier($columnName),
                    QuoteHelper::quoteIdentifier($columnName),
                    QuoteHelper::quoteIdentifier($columnName)
                );
            } else {
                $columnsSet[] = sprintf(
                    '%s = COALESCE("src".%s, \'\')',
                    QuoteHelper::quoteIdentifier($columnName),
                    QuoteHelper::quoteIdentifier($columnName)
                );
            }
        }

        if ($importOptions->useTimestamp()) {
            $columnsSet[] = sprintf(
                '%s = \'%s\'',
                QuoteHelper::quoteIdentifier(self::TIMESTAMP_COLUMN_NAME),
                DateTimeHelper::getNowFormatted()
            );
        }

        // update only changed rows - mysql TIMESTAMP ON UPDATE behaviour simulation
        $columnsComparsionSql = array_map(
            function ($columnName) {
                return sprintf(
                    'COALESCE(TO_VARCHAR("dest".%s), \'\') != COALESCE("src".%s, \'\')',
                    QuoteHelper::quoteIdentifier($columnName),
                    QuoteHelper::quoteIdentifier($columnName)
                );
            },
            $importOptions->getColumns()
        );

        $sql = sprintf(
            'UPDATE %s AS "dest" SET %s FROM %s.%s AS "src" WHERE %s AND (%s) ',
            $importOptions->getTargetTableWithScheme(),
            implode(', ', $columnsSet),
            QuoteHelper::quoteIdentifier($importOptions->getSchema()),
            QuoteHelper::quoteIdentifier($stagingTableName),
            self::buildPrimayKeyWhereConditions($primaryKeys),
            implode(' OR ', $columnsComparsionSql)
        );

        return $sql;
    }
}
