<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable;

use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;

class SqlBuilder
{
    private const SRC_ALIAS = 'src';

    private function assertColumnExist(
        BigqueryTableDefinition $tableDefinition,
        BigqueryColumn $columnDefinition,
    ): BigqueryColumn {
        $destinationColumn = null;
        // case sensitive search
        /** @var BigqueryColumn $col */
        foreach ($tableDefinition->getColumnsDefinitions() as $col) {
            if ($col->getColumnName() === $columnDefinition->getColumnName()) {
                $destinationColumn = $col;
                break;
            }
        }
        if ($destinationColumn === null) {
            throw new Exception(
                sprintf(
                    'Columns "%s" can be imported as it was not found between columns "%s" of destination table.',
                    $columnDefinition->getColumnName(),
                    implode(', ', $tableDefinition->getColumnsNames()),
                ),
                Exception::UNKNOWN_ERROR,
            );
        }

        return $destinationColumn;
    }

    public function getBeginTransaction(): string
    {
        return 'BEGIN TRANSACTION';
    }

    public function getCommitTransaction(): string
    {
        return 'COMMIT TRANSACTION';
    }

    public function getRollbackTransaction(): string
    {
        return 'ROLLBACK TRANSACTION;';
    }

    /**
     * SQL to drop table. DOES NOT check existence of table
     *
     */
    public function getDropTableUnsafe(string $dbName, string $tableName): string
    {
        return sprintf(
            'DROP TABLE %s.%s',
            BigqueryQuote::quoteSingleIdentifier($dbName),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        );
    }

    public function getDropTableIfExistsCommand(
        string $schema,
        string $tableName,
    ): string {
        return sprintf(
            'DROP TABLE IF EXISTS %s.%s',
            BigqueryQuote::quoteSingleIdentifier($schema),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        );
    }

    /**
     * @return string[]
     */
    private function getColumnSetSqlPartForStringTable(
        BigqueryTableDefinition $sourceTableDefinition,
        BigqueryTableDefinition $destinationTableDefinition,
        BigqueryImportOptions $importOptions,
    ): array {
        $columnsSetSql = [];
        /** @var BigqueryColumn $columnDefinition */
        foreach ($sourceTableDefinition->getColumnsDefinitions() as $columnDefinition) {
            $destinationColumn = $this->assertColumnExist($destinationTableDefinition, $columnDefinition);
            if (in_array($columnDefinition->getColumnName(), $importOptions->getConvertEmptyValuesToNull(), true)) {
                // use nullif only for string base type
                if ($columnDefinition->getColumnDefinition()->getBasetype() === BaseType::STRING) {
                    $columnsSetSql[] = sprintf(
                        'NULLIF(%s.%s, \'\')',
                        BigqueryQuote::quoteSingleIdentifier(self::SRC_ALIAS),
                        BigqueryQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                    );
                } else {
                    $columnsSetSql[] = BigqueryQuote::quoteSingleIdentifier($columnDefinition->getColumnName());
                }
            } elseif ($columnDefinition->getColumnDefinition()->getBasetype() === BaseType::STRING) {
                $columnsSetSql[] = sprintf(
                    'CAST(COALESCE(%s.%s, \'\') as %s) AS %s',
                    BigqueryQuote::quoteSingleIdentifier(self::SRC_ALIAS),
                    BigqueryQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                    $destinationColumn->getColumnDefinition()->getType(),
                    BigqueryQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                );
            } else {
                // on columns other than string dont use COALESCE, use direct cast
                // this will fail if the column is not null, but this is expected
                $columnsSetSql[] = sprintf(
                    'CAST(%s.%s as %s) AS %s',
                    BigqueryQuote::quoteSingleIdentifier(self::SRC_ALIAS),
                    BigqueryQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                    $destinationColumn->getColumnDefinition()->getType(),
                    BigqueryQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                );
            }
        }
        return $columnsSetSql;
    }

    public function getTableExistsCommand(string $dbName, string $tableName): string
    {
        return sprintf(
            <<< SQL
SELECT COUNT(*) AS count FROM %s.INFORMATION_SCHEMA.TABLES WHERE `table_type` != 'VIEW' AND table_name = %s;
SQL,
            BigqueryQuote::quoteSingleIdentifier($dbName),
            BigqueryQuote::quote($tableName),
        );
    }

    public function getInsertAllIntoTargetTableCommand(
        BigqueryTableDefinition $sourceTableDefinition,
        BigqueryTableDefinition $destinationTableDefinition,
        BigqueryImportOptions $importOptions,
        string $timestamp,
    ): string {
        $destinationTable = sprintf(
            '%s.%s',
            BigqueryQuote::quoteSingleIdentifier($destinationTableDefinition->getSchemaName()),
            BigqueryQuote::quoteSingleIdentifier($destinationTableDefinition->getTableName()),
        );

        $columnsToInsert = $sourceTableDefinition->getColumnsNames();
        $useTimestamp = !in_array(ToStageImporterInterface::TIMESTAMP_COLUMN_NAME, $columnsToInsert, true)
            && $importOptions->useTimestamp();

        if ($useTimestamp) {
            $columnsToInsert = array_merge(
                $sourceTableDefinition->getColumnsNames(),
                [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
            );
        }

        if ($importOptions->usingUserDefinedTypes()) {
            $columnsSetSql = [];
            /** @var BigqueryColumn $columnDefinition */
            foreach ($sourceTableDefinition->getColumnsDefinitions() as $columnDefinition) {
                $this->assertColumnExist($destinationTableDefinition, $columnDefinition);
                $columnsSetSql[] = sprintf(
                    '%s.%s',
                    BigqueryQuote::quoteSingleIdentifier(self::SRC_ALIAS),
                    BigqueryQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                );
            }
        } else {
            $columnsSetSql = $this->getColumnSetSqlPartForStringTable(
                $sourceTableDefinition,
                $destinationTableDefinition,
                $importOptions,
            );
        }

        if ($useTimestamp) {
            $columnsSetSql[] = sprintf(
                'CAST(%s as %s)',
                BigqueryQuote::quote($timestamp),
                Bigquery::TYPE_TIMESTAMP,
            );
        }

        return sprintf(
            'INSERT INTO %s (%s) SELECT %s FROM %s.%s AS %s',
            $destinationTable,
            $this->getColumnsString($columnsToInsert),
            implode(',', $columnsSetSql),
            BigqueryQuote::quoteSingleIdentifier($sourceTableDefinition->getSchemaName()),
            BigqueryQuote::quoteSingleIdentifier($sourceTableDefinition->getTableName()),
            BigqueryQuote::quoteSingleIdentifier(self::SRC_ALIAS),
        );
    }

    public function getTruncateTable(string $schema, string $tableName): string
    {
        return sprintf(
            'TRUNCATE TABLE %s.%s',
            BigqueryQuote::quoteSingleIdentifier($schema),
            BigqueryQuote::quoteSingleIdentifier($tableName),
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
            return $alias . BigqueryQuote::quoteSingleIdentifier($columns);
        }, $columns));
    }

    public function getDeleteOldItemsCommand(
        BigqueryTableDefinition $stagingTableDefinition,
        BigqueryTableDefinition $destinationTableDefinition,
        BigqueryImportOptions $importOptions,
    ): string {
        $stagingTable = sprintf(
            '%s.%s',
            BigqueryQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            BigqueryQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName()),
        );

        $destinationTable = sprintf(
            '%s.%s',
            BigqueryQuote::quoteSingleIdentifier($destinationTableDefinition->getSchemaName()),
            BigqueryQuote::quoteSingleIdentifier($destinationTableDefinition->getTableName()),
        );

        return sprintf(
            'DELETE %s AS `src` WHERE EXISTS (SELECT * FROM %s AS `dest` WHERE %s)',
            $stagingTable,
            $destinationTable,
            $this->getPrimaryKeyWhereConditions(
                $destinationTableDefinition->getPrimaryKeysNames(),
                $importOptions,
            ),
        );
    }

    /**
     * @param string[] $primaryKeys
     */
    public function getCreateDedupTable(
        BigqueryTableDefinition $stagingTableDefinition,
        string $dedupTableName,
        array $primaryKeys,
    ): string {
        return sprintf(
            <<< SQL
CREATE OR REPLACE TABLE %s.%s AS
%s
SQL,
            BigqueryQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            BigqueryQuote::quoteSingleIdentifier($dedupTableName),
            $this->getDedupSelect(
                $stagingTableDefinition,
                $stagingTableDefinition->getColumnsNames(),
                $primaryKeys,
            ),
        );
    }

    public function getUpdateWithPkCommand(
        BigqueryTableDefinition $stagingTableDefinition,
        BigqueryTableDefinition $destinationTableDefinition,
        BigqueryImportOptions $importOptions,
        string $timestampValue,
    ): string {
        $columnsSet = [];

        foreach ($stagingTableDefinition->getColumnsNames() as $columnName) {
            if ($importOptions->usingUserDefinedTypes()) {
                $columnsSet[] = sprintf(
                    '%s = `src`.%s',
                    BigqueryQuote::quoteSingleIdentifier($columnName),
                    BigqueryQuote::quoteSingleIdentifier($columnName),
                );
                continue;
            }
            // if string table convert nulls<=>''
            if (in_array($columnName, $importOptions->getConvertEmptyValuesToNull(), true)) {
                $columnsSet[] = sprintf(
                    '%s = IF(`src`.%s = \'\', NULL, `src`.%s)',
                    BigqueryQuote::quoteSingleIdentifier($columnName),
                    BigqueryQuote::quoteSingleIdentifier($columnName),
                    BigqueryQuote::quoteSingleIdentifier($columnName),
                );
            } else {
                $columnsSet[] = sprintf(
                    '%s = COALESCE(`src`.%s, \'\')',
                    BigqueryQuote::quoteSingleIdentifier($columnName),
                    BigqueryQuote::quoteSingleIdentifier($columnName),
                );
            }
        }

        if ($importOptions->useTimestamp()) {
            $columnsSet[] = sprintf(
                '%s = \'%s\'',
                BigqueryQuote::quoteSingleIdentifier(ToStageImporterInterface::TIMESTAMP_COLUMN_NAME),
                $timestampValue,
            );
        }

        if ($importOptions->usingUserDefinedTypes()) {
            $columnsComparisonSql = array_map(
                static function ($columnName) {
                    return sprintf(
                        '`dest`.%s IS DISTINCT FROM `src`.%s',
                        BigqueryQuote::quoteSingleIdentifier($columnName),
                        BigqueryQuote::quoteSingleIdentifier($columnName),
                    );
                },
                $stagingTableDefinition->getColumnsNames(),
            );
        } else {
            $columnsComparisonSql = array_map(
                static function ($columnName) {
                    return sprintf(
                        '`dest`.%s != COALESCE(`src`.%s, \'\')',
                        BigqueryQuote::quoteSingleIdentifier($columnName),
                        BigqueryQuote::quoteSingleIdentifier($columnName),
                    );
                },
                $stagingTableDefinition->getColumnsNames(),
            );
        }

        $dest = sprintf(
            '%s.%s',
            BigqueryQuote::quoteSingleIdentifier($destinationTableDefinition->getSchemaName()),
            BigqueryQuote::quoteSingleIdentifier($destinationTableDefinition->getTableName()),
        );

        return sprintf(
            'UPDATE %s AS `dest` SET %s FROM %s.%s AS `src` WHERE %s AND (%s)',
            $dest,
            implode(', ', $columnsSet),
            BigqueryQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            BigqueryQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName()),
            $this->getPrimaryKeyWhereConditions($destinationTableDefinition->getPrimaryKeysNames(), $importOptions),
            implode(' OR ', $columnsComparisonSql),
        );
    }

    /**
     * @param string[] $primaryKeys
     */
    private function getPrimaryKeyWhereConditions(
        array $primaryKeys,
        BigqueryImportOptions $importOptions,
    ): string {
        $pkWhereSql = array_map(function (string $col) use ($importOptions) {
            $str = '`dest`.%s = COALESCE(`src`.%s, \'\')';
            if ($importOptions->usingUserDefinedTypes()) {
                $str = '`dest`.%s = `src`.%s';
            }
            return sprintf(
                $str,
                BigqueryQuote::quoteSingleIdentifier($col),
                BigqueryQuote::quoteSingleIdentifier($col),
            );
        }, $primaryKeys);

        return implode(' AND ', $pkWhereSql) . ' ';
    }

    /**
     * @param string[] $primaryKeysNames
     */
    public function getDedupCommand(
        BigqueryTableDefinition $stagingTableDefinition,
        BigqueryTableDefinition $deduplicationTableDefinition,
        array $primaryKeysNames,
    ): string {
        if (empty($primaryKeysNames)) {
            return '';
        }

        $depudeSql = $this->getDedupSelect(
            $stagingTableDefinition,
            $deduplicationTableDefinition->getColumnsNames(),
            $primaryKeysNames,
        );

        $deduplication = sprintf(
            '%s.%s',
            BigqueryQuote::quoteSingleIdentifier($deduplicationTableDefinition->getSchemaName()),
            BigqueryQuote::quoteSingleIdentifier($deduplicationTableDefinition->getTableName()),
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
     * @param string[] $primaryKeysNames
     */
    private function getDedupSelect(
        BigqueryTableDefinition $stagingTableDefinition,
        array $columns,
        array $primaryKeysNames,
    ): string {
        $pkSql = $this->getColumnsString(
            $primaryKeysNames,
            ',',
            self::SRC_ALIAS,
        );

        // Order by all columns for deterministic tie-breaking when duplicate PKs exist
        $allColumnsSql = $this->getColumnsString(
            $columns,
            ',',
            self::SRC_ALIAS,
        );

        $stage = sprintf(
            '%s.%s',
            BigqueryQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            BigqueryQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName()),
        );

        return sprintf(
            <<<SQL
SELECT %s FROM (
    SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s DESC) AS `_row_number_`
    FROM %s as %s
) AS a
    WHERE a.`_row_number_` = 1
SQL,
            $this->getColumnsString($columns, ',', 'a'),
            $this->getColumnsString($columns, ', ', self::SRC_ALIAS),
            $pkSql,
            $allColumnsSql,
            $stage,
            self::SRC_ALIAS,
        );
    }
}
