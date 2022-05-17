<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Keboola\Db\ImportExport\Backend\Snowflake\Helper\ColumnsHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage\Snowflake\Table;
use Keboola\Db\ImportExport\Storage\SourceInterface;

class SqlCommandBuilder
{
    public function getBeginTransaction(): string
    {
        return 'BEGIN TRANSACTION';
    }

    public function getCommitTransaction(): string
    {
        return 'COMMIT';
    }

    /**
     * @param string[] $columns
     */
    public function getCreateStagingTableCommand(
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

    /**
     * @param string[] $primaryKeys
     */
    public function getDedupCommand(
        SourceInterface $source,
        Table $destination,
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
            . 'SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) AS "_row_number_"'
            . 'FROM %s.%s'
            . ') AS a '
            . 'WHERE a."_row_number_" = 1',
            ColumnsHelper::getColumnsString($source->getColumnsNames(), ',', 'a'),
            ColumnsHelper::getColumnsString($source->getColumnsNames(), ', '),
            $pkSql,
            $pkSql,
            QuoteHelper::quoteIdentifier($destination->getSchema()),
            QuoteHelper::quoteIdentifier($stagingTableName)
        );

        return sprintf(
            'INSERT INTO %s.%s (%s) %s',
            QuoteHelper::quoteIdentifier($destination->getSchema()),
            QuoteHelper::quoteIdentifier($tempTableName),
            ColumnsHelper::getColumnsString($source->getColumnsNames()),
            $depudeSql
        );
    }

    /**
     * @param string[] $primaryKeys
     */
    public function getDeleteOldItemsCommand(
        Table $destination,
        string $stagingTableName,
        array $primaryKeys
    ): string {
        // Delete updated rows from staging table
        return sprintf(
            'DELETE FROM %s.%s "src" USING %s AS "dest" WHERE %s',
            QuoteHelper::quoteIdentifier($destination->getSchema()),
            QuoteHelper::quoteIdentifier($stagingTableName),
            $destination->getQuotedTableWithScheme(),
            $this->getPrimayKeyWhereConditions($primaryKeys)
        );
    }

    /**
     * @param string[] $primaryKeys
     */
    private function getPrimayKeyWhereConditions(
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

    public function getDropCommand(
        string $schema,
        string $tableName
    ): string {
        return sprintf(
            'DROP TABLE %s.%s',
            QuoteHelper::quoteIdentifier($schema),
            QuoteHelper::quoteIdentifier($tableName)
        );
    }

    public function getInsertAllIntoTargetTableCommand(
        SourceInterface $source,
        Table $destination,
        ImportOptionsInterface $importOptions,
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
        }, $source->getColumnsNames()));

        if (in_array(Importer::TIMESTAMP_COLUMN_NAME, $source->getColumnsNames())
            || $importOptions->useTimestamp() === false
        ) {
            return sprintf(
                'INSERT INTO %s (%s) (SELECT %s FROM %s.%s)',
                $destination->getQuotedTableWithScheme(),
                ColumnsHelper::getColumnsString($source->getColumnsNames()),
                $columnsSetSqlSelect,
                QuoteHelper::quoteIdentifier($destination->getSchema()),
                QuoteHelper::quoteIdentifier($stagingTableName)
            );
        }

        return sprintf(
            'INSERT INTO %s (%s, "%s") (SELECT %s, \'%s\' FROM %s.%s)',
            $destination->getQuotedTableWithScheme(),
            ColumnsHelper::getColumnsString($source->getColumnsNames()),
            Importer::TIMESTAMP_COLUMN_NAME,
            $columnsSetSqlSelect,
            DateTimeHelper::getNowFormatted(),
            QuoteHelper::quoteIdentifier($destination->getSchema()),
            QuoteHelper::quoteIdentifier($stagingTableName)
        );
    }

    public function getInsertFromStagingToTargetTableCommand(
        SourceInterface $source,
        Table $destination,
        ImportOptionsInterface $importOptions,
        string $stagingTableName,
        string $timestampValue
    ): string {
        if ($importOptions->useTimestamp()) {
            $insColumns = array_merge($source->getColumnsNames(), [Importer::TIMESTAMP_COLUMN_NAME]);
        } else {
            $insColumns = $source->getColumnsNames();
        }

        $columnsSetSql = [];

        foreach ($source->getColumnsNames() as $columnName) {
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
            $columnsSetSql[] = sprintf('\'%s\'', $timestampValue);
        }

        return sprintf(
            'INSERT INTO %s (%s) SELECT %s FROM %s.%s AS "src"',
            $destination->getQuotedTableWithScheme(),
            ColumnsHelper::getColumnsString($insColumns),
            implode(',', $columnsSetSql),
            QuoteHelper::quoteIdentifier($destination->getSchema()),
            QuoteHelper::quoteIdentifier($stagingTableName)
        );
    }

    public function getRenameTableCommand(
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

    public function getTableItemsCountCommand(string $schema, string $table): string
    {
        return sprintf(
            'SELECT COUNT(*) AS "count" FROM %s.%s',
            QuoteHelper::quoteIdentifier($schema),
            QuoteHelper::quoteIdentifier($table)
        );
    }

    public function getTruncateTableCommand(
        string $schema,
        string $tableName
    ): string {
        return sprintf(
            'TRUNCATE %s.%s',
            QuoteHelper::quoteIdentifier($schema),
            QuoteHelper::quoteIdentifier($tableName)
        );
    }

    /**
     * @param string[] $primaryKeys
     */
    public function getUpdateWithPkCommand(
        SourceInterface $source,
        Table $destination,
        ImportOptionsInterface $importOptions,
        string $stagingTableName,
        array $primaryKeys,
        string $timestamp
    ): string {
        $columnsSet = [];
        foreach ($source->getColumnsNames() as $columnName) {
            if (in_array($columnName, $importOptions->getConvertEmptyValuesToNull(), true)) {
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
                QuoteHelper::quoteIdentifier(Importer::TIMESTAMP_COLUMN_NAME),
                $timestamp
            );
        }

        // update only changed rows - mysql TIMESTAMP ON UPDATE behaviour simulation
        $columnsComparisionSql = array_map(
            static function ($columnName) {
                return sprintf(
                    'COALESCE(TO_VARCHAR("dest".%s), \'\') != COALESCE("src".%s, \'\')',
                    QuoteHelper::quoteIdentifier($columnName),
                    QuoteHelper::quoteIdentifier($columnName)
                );
            },
            $source->getColumnsNames()
        );

        return sprintf(
            'UPDATE %s AS "dest" SET %s FROM %s.%s AS "src" WHERE %s AND (%s) ',
            $destination->getQuotedTableWithScheme(),
            implode(', ', $columnsSet),
            QuoteHelper::quoteIdentifier($destination->getSchema()),
            QuoteHelper::quoteIdentifier($stagingTableName),
            $this->getPrimayKeyWhereConditions($primaryKeys),
            implode(' OR ', $columnsComparisionSql)
        );
    }
}
