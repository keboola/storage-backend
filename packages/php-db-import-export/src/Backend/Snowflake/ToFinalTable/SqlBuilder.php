<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable;

use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage\Snowflake\Table;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;

class SqlBuilder
{
    public const SRC_ALIAS = 'src';

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
        array $primaryKeys
    ): string {
        if (empty($primaryKeys)) {
            return '';
        }

        $pkSql = $this->getColumnsString(
            $primaryKeys,
            ','
        );

        $stage = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName())
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
            $stage
        );

        $deduplication = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($deduplicationTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($deduplicationTableDefinition->getTableName())
        );

        return sprintf(
            'INSERT INTO %s (%s) %s',
            $deduplication,
            $this->getColumnsString($deduplicationTableDefinition->getColumnsNames()),
            $depudeSql
        );
    }

    /**
     * @param string[] $columns
     */
    public function getColumnsString(
        array $columns,
        string $delimiter = ', ',
        ?string $tableAlias = null
    ): string {
        return implode($delimiter, array_map(static function ($columns) use (
            $tableAlias
        ) {
            $alias = $tableAlias === null ? '' : $tableAlias . '.';
            return $alias . SnowflakeQuote::quoteSingleIdentifier($columns);
        }, $columns));
    }

    public function getDeleteOldItemsCommand(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition,
        SnowflakeImportOptions $importOptions
    ): string {
        $stagingTable = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName())
        );

        $destinationTable = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getTableName())
        );

        return sprintf(
            'DELETE FROM %s "src" USING %s AS "dest" WHERE %s',
            $stagingTable,
            $destinationTable,
            $this->getPrimayKeyWhereConditions(
                $destinationTableDefinition->getPrimaryKeysNames(),
                $importOptions
            )
        );
    }

    /**
     * @param string[] $primaryKeys
     */
    private function getPrimayKeyWhereConditions(
        array $primaryKeys,
        SnowflakeImportOptions $importOptions
    ): string {
        $pkWhereSql = array_map(function (string $col) use ($importOptions) {
            $str = '"dest".%s = COALESCE("src".%s, \'\')';
            if ($importOptions->isRequireSameTables() === true) {
                $str = '"dest".%s = "src".%s';
            }
            return sprintf(
                $str,
                QuoteHelper::quoteIdentifier($col),
                QuoteHelper::quoteIdentifier($col)
            );
        }, $primaryKeys);

        return implode(' AND ', $pkWhereSql) . ' ';
    }


    public function getDropTableIfExistsCommand(
        string $schema,
        string $tableName
    ): string {
        return sprintf(
            'DROP TABLE IF EXISTS %s.%s',
            SnowflakeQuote::quoteSingleIdentifier($schema),
            SnowflakeQuote::quoteSingleIdentifier($tableName)
        );
    }

    public function getInsertAllIntoTargetTableCommand(
        SnowflakeTableDefinition $sourceTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition,
        SnowflakeImportOptions $importOptions,
        string $timestamp
    ): string {
        $destinationTable = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getTableName())
        );

        $insColumns = $sourceTableDefinition->getColumnsNames();
        $useTimestamp = !in_array(ToStageImporterInterface::TIMESTAMP_COLUMN_NAME, $insColumns, true)
            && $importOptions->useTimestamp();

        if ($useTimestamp) {
            $insColumns = array_merge(
                $sourceTableDefinition->getColumnsNames(),
                [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME]
            );
        }

        $columnsSetSql = [];

        /** @var SnowflakeColumn $columnDefinition */
        foreach ($sourceTableDefinition->getColumnsDefinitions() as $columnDefinition) {
            // output mapping same tables are required do not convert nulls to empty strings
            if ($importOptions->isRequireSameTables() === true) {
                $columnsSetSql[] = SnowflakeQuote::quoteSingleIdentifier($columnDefinition->getColumnName());
                continue;
            }

            // Input mapping convert empty values to null
            // empty strings '' are converted to null values
            if (in_array($columnDefinition->getColumnName(), $importOptions->getConvertEmptyValuesToNull(), true)) {
                // use nullif only for string base type
                if ($columnDefinition->getColumnDefinition()->getBasetype() === BaseType::STRING) {
                    $columnsSetSql[] = sprintf(
                        'IFF(%s = \'\', NULL, %s)',
                        SnowflakeQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                        SnowflakeQuote::quoteSingleIdentifier($columnDefinition->getColumnName())
                    );
                    continue;
                }
                // if tables is not typed column could be other than string in this case we skip conversion
                $columnsSetSql[] = SnowflakeQuote::quoteSingleIdentifier($columnDefinition->getColumnName());
                continue;
            }

            // for string base type convert null values to empty string ''
            //phpcs:ignore
            // TODO: coalesce could be skipped in input mapping to workspace for typed and non typed tables https://keboola.atlassian.net/browse/KBC-2886
            if ($columnDefinition->getColumnDefinition()->getBasetype() === BaseType::STRING) {
                $columnsSetSql[] = sprintf(
                    'COALESCE(%s, \'\') AS %s',
                    SnowflakeQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                    SnowflakeQuote::quoteSingleIdentifier($columnDefinition->getColumnName())
                );
                continue;
            }
            // on columns other than string dont use COALESCE
            // this will fail if the column is not null, but this is expected
            $columnsSetSql[] = SnowflakeQuote::quoteSingleIdentifier($columnDefinition->getColumnName());
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
            SnowflakeQuote::quoteSingleIdentifier(self::SRC_ALIAS)
        );
    }

    public function getTruncateTable(
        string $schema,
        string $tableName
    ): string {
        return sprintf(
            'TRUNCATE TABLE %s.%s',
            SnowflakeQuote::quoteSingleIdentifier($schema),
            SnowflakeQuote::quoteSingleIdentifier($tableName)
        );
    }

    public function getUpdateWithPkCommand(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $destinationDefinition,
        SnowflakeImportOptions $importOptions,
        string $timestamp
    ): string {
        $columnsSet = [];

        foreach ($stagingTableDefinition->getColumnsNames() as $columnName) {
            if ($importOptions->isRequireSameTables() === true) {
                $columnsSet[] = sprintf(
                    '%s = "src".%s',
                    SnowflakeQuote::quoteSingleIdentifier($columnName),
                    SnowflakeQuote::quoteSingleIdentifier($columnName),
                );
                continue;
            }
            if (in_array($columnName, $importOptions->getConvertEmptyValuesToNull(), true)) {
                $columnsSet[] = sprintf(
                    '%s = IFF("src".%s = \'\', NULL, "src".%s)',
                    SnowflakeQuote::quoteSingleIdentifier($columnName),
                    SnowflakeQuote::quoteSingleIdentifier($columnName),
                    SnowflakeQuote::quoteSingleIdentifier($columnName)
                );
            } else {
                $columnsSet[] = sprintf(
                    '%s = COALESCE("src".%s, \'\')',
                    SnowflakeQuote::quoteSingleIdentifier($columnName),
                    SnowflakeQuote::quoteSingleIdentifier($columnName)
                );
            }
        }

        if ($importOptions->useTimestamp()) {
            $columnsSet[] = sprintf(
                '%s = \'%s\'',
                SnowflakeQuote::quoteSingleIdentifier(ToStageImporterInterface::TIMESTAMP_COLUMN_NAME),
                $timestamp
            );
        }

        // update only changed rows - mysql TIMESTAMP ON UPDATE behaviour simulation
        if ($importOptions->isRequireSameTables() === true) {
            $columnsComparisonSql = array_map(
                static function ($columnName) {
                    return sprintf(
                        '"dest".%s != "src".%s',
                        SnowflakeQuote::quoteSingleIdentifier($columnName),
                        SnowflakeQuote::quoteSingleIdentifier($columnName)
                    );
                },
                $stagingTableDefinition->getColumnsNames()
            );
        } else {
            $columnsComparisonSql = array_map(
                static function ($columnName) {
                    return sprintf(
                        'COALESCE(TO_VARCHAR("dest".%s), \'\') != COALESCE("src".%s, \'\')',
                        SnowflakeQuote::quoteSingleIdentifier($columnName),
                        SnowflakeQuote::quoteSingleIdentifier($columnName)
                    );
                },
                $stagingTableDefinition->getColumnsNames()
            );
        }

        $dest = sprintf(
            '%s.%s',
            ExasolQuote::quoteSingleIdentifier($destinationDefinition->getSchemaName()),
            ExasolQuote::quoteSingleIdentifier($destinationDefinition->getTableName())
        );
        return sprintf(
            'UPDATE %s AS "dest" SET %s FROM %s.%s AS "src" WHERE %s AND (%s)',
            $dest,
            implode(', ', $columnsSet),
            QuoteHelper::quoteIdentifier($stagingTableDefinition->getSchemaName()),
            QuoteHelper::quoteIdentifier($stagingTableDefinition->getTableName()),
            $this->getPrimayKeyWhereConditions($destinationDefinition->getPrimaryKeysNames(), $importOptions),
            implode(' OR ', $columnsComparisonSql)
        );
    }
}
