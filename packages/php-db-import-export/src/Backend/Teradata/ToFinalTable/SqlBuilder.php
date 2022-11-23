<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable;

use Exception as InternalException;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Teradata;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;

class SqlBuilder
{
    private const SRC_ALIAS = 'src';

    public function getEndTransaction(): string
    {
        return 'END TRANSACTION';
    }

    public function getBeginTransaction(): string
    {
        return 'BEGIN TRANSACTION';
    }

    /**
     * SQL to drop table. DOES NOT check existence of table
     *
     */
    public function getDropTableUnsafe(string $dbName, string $tableName): string
    {
        return sprintf(
            'DROP TABLE %s.%s',
            TeradataQuote::quoteSingleIdentifier($dbName),
            TeradataQuote::quoteSingleIdentifier($tableName)
        );
    }

    public function getTableExistsCommand(string $dbName, string $tableName): string
    {
        return sprintf(
            'SELECT COUNT(*) FROM DBC.TablesVX WHERE DatabaseName = %s AND TableName = %s;',
            TeradataQuote::quote($dbName),
            TeradataQuote::quote($tableName)
        );
    }

    public function getInsertAllIntoTargetTableCommand(
        TeradataTableDefinition $sourceTableDefinition,
        TeradataTableDefinition $destinationTableDefinition,
        TeradataImportOptions $importOptions,
        string $timestamp
    ): string {
        $destinationTable = sprintf(
            '%s.%s',
            TeradataQuote::quoteSingleIdentifier($destinationTableDefinition->getSchemaName()),
            TeradataQuote::quoteSingleIdentifier($destinationTableDefinition->getTableName())
        );

        $columnsToInsert = $sourceTableDefinition->getColumnsNames();
        $useTimestamp = !in_array(ToStageImporterInterface::TIMESTAMP_COLUMN_NAME, $columnsToInsert, true)
            && $importOptions->useTimestamp();

        if ($useTimestamp) {
            $columnsToInsert = array_merge(
                $sourceTableDefinition->getColumnsNames(),
                [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME]
            );
        }

        $columnsSetSql = [];

        /** @var TeradataColumn $columnDefinition */
        foreach ($sourceTableDefinition->getColumnsDefinitions() as $columnDefinition) {
            $destinationColumn = null;
            // case sensitive search
            /** @var TeradataColumn $col */
            foreach ($destinationTableDefinition->getColumnsDefinitions() as $col) {
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
                        implode(', ', $destinationTableDefinition->getColumnsNames())
                    ),
                    Exception::UNKNOWN_ERROR
                );
            }
            if (in_array($columnDefinition->getColumnName(), $importOptions->getConvertEmptyValuesToNull(), true)) {
                // use nullif only for string base type
                if ($columnDefinition->getColumnDefinition()->getBasetype() === BaseType::STRING) {
                    $columnsSetSql[] = sprintf(
                        'NULLIF(%s, \'\')',
                        TeradataQuote::quoteSingleIdentifier($columnDefinition->getColumnName())
                    );
                } else {
                    $columnsSetSql[] = TeradataQuote::quoteSingleIdentifier($columnDefinition->getColumnName());
                }
            } elseif ($columnDefinition->getColumnDefinition()->getBasetype() === BaseType::STRING) {
                $columnsSetSql[] = sprintf(
                    'CAST(COALESCE(%s, \'\') as %s) AS %s',
                    TeradataQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                    $destinationColumn->getColumnDefinition()->buildTypeWithLength(),
                    TeradataQuote::quoteSingleIdentifier($columnDefinition->getColumnName())
                );
            } else {
                // on columns other than string dont use COALESCE, use direct cast
                // this will fail if the column is not null, but this is expected
                $columnsSetSql[] = sprintf(
                    'CAST(%s as %s) AS %s',
                    TeradataQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                    $destinationColumn->getColumnDefinition()->buildTypeWithLength(),
                    TeradataQuote::quoteSingleIdentifier($columnDefinition->getColumnName())
                );
            }
        }

        if ($useTimestamp) {
            $columnsSetSql[] = TeradataQuote::quote($timestamp);
        }

        return sprintf(
            'INSERT INTO %s (%s) SELECT %s FROM %s.%s AS %s',
            $destinationTable,
            $this->getColumnsString($columnsToInsert),
            implode(',', $columnsSetSql),
            TeradataQuote::quoteSingleIdentifier($sourceTableDefinition->getSchemaName()),
            TeradataQuote::quoteSingleIdentifier($sourceTableDefinition->getTableName()),
            TeradataQuote::quoteSingleIdentifier(self::SRC_ALIAS)
        );
    }

    /**
     * @param string[] $primaryKeys
     */
    public function getDedupCommand(
        TeradataTableDefinition $stagingTableDefinition,
        TeradataTableDefinition $deduplicationTableDefinition,
        array $primaryKeys
    ): string {
        if (empty($primaryKeys)) {
            return '';
        }

        $deduplication = sprintf(
            '%s.%s',
            TeradataQuote::quoteSingleIdentifier($deduplicationTableDefinition->getSchemaName()),
            TeradataQuote::quoteSingleIdentifier($deduplicationTableDefinition->getTableName())
        );

        return sprintf(
            'INSERT INTO %s (%s) %s',
            $deduplication,
            $this->getColumnsString($deduplicationTableDefinition->getColumnsNames()),
            $this->getDedupSql($stagingTableDefinition, $deduplicationTableDefinition, $primaryKeys)
        );
    }

    /**
     * @param string[] $primaryKeys
     */
    public function getDedupSql(
        TeradataTableDefinition $stagingTableDefinition,
        TeradataTableDefinition $deduplicationTableDefinition,
        array $primaryKeys
    ): string {
        $pkSql = $this->getColumnsString(
            $primaryKeys,
            ','
        );

        $stage = sprintf(
            '%s.%s',
            TeradataQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            TeradataQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName())
        );

         return sprintf(
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
    }

    public function getTruncateTableWithDeleteCommand(
        string $schema,
        string $tableName
    ): string {
        // TD has no TRUNCATE command - DELETE ALL has to be used
        return sprintf(
            'DELETE %s.%s ALL',
            TeradataQuote::quoteSingleIdentifier($schema),
            TeradataQuote::quoteSingleIdentifier($tableName)
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
            return $alias . TeradataQuote::quoteSingleIdentifier($columns);
        }, $columns));
    }

    public function getDeleteOldItemsCommand(
        TeradataTableDefinition $stagingTableDefinition,
        TeradataTableDefinition $destinationTableDefinition,
        TeradataImportOptions $importOptions
    ): string {
        $stagingTable = sprintf(
            '%s.%s',
            TeradataQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            TeradataQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName())
        );

        $destinationTable = sprintf(
            '%s.%s',
            TeradataQuote::quoteSingleIdentifier($destinationTableDefinition->getSchemaName()),
            TeradataQuote::quoteSingleIdentifier($destinationTableDefinition->getTableName())
        );

        return sprintf(
            'DELETE %s FROM %s AS "joined" WHERE %s',
            $stagingTable,
            $destinationTable,
            $this->getPrimayKeyWhereConditions(
                $destinationTableDefinition->getPrimaryKeysNames(),
                $importOptions,
                $stagingTable,
                '"joined"'
            )
        );
    }

    public function getUpdateWithPkCommand(
        TeradataTableDefinition $stagingTableDefinition,
        TeradataTableDefinition $destinationDefinition,
        TeradataImportOptions $importOptions,
        string $timestamp
    ): string {
        $columnsSet = [];
        $dest = sprintf(
            '%s.%s',
            TeradataQuote::quoteSingleIdentifier($destinationDefinition->getSchemaName()),
            TeradataQuote::quoteSingleIdentifier($destinationDefinition->getTableName())
        );

        foreach ($stagingTableDefinition->getColumnsNames() as $columnName) {
            if (!$importOptions->isNullManipulationEnabled()) {
                $columnsSet[] = sprintf(
                    '%s = "src".%s',
                    TeradataQuote::quoteSingleIdentifier($columnName),
                    TeradataQuote::quoteSingleIdentifier($columnName),
                );
                continue;
            }
            if (in_array($columnName, $importOptions->getConvertEmptyValuesToNull(), true)) {
                // values '' values from staging convert to NULL
                $columnsSet[] = sprintf(
                    '%s = CASE WHEN "src".%s = \'\' THEN NULL ELSE "src".%s END',
                    TeradataQuote::quoteSingleIdentifier($columnName),
                    TeradataQuote::quoteSingleIdentifier($columnName),
                    TeradataQuote::quoteSingleIdentifier($columnName)
                );
            } else {
                $columnsSet[] = sprintf(
                    '%s = COALESCE("src".%s, \'\')',
                    TeradataQuote::quoteSingleIdentifier($columnName),
                    TeradataQuote::quoteSingleIdentifier($columnName)
                );
            }
        }

        if ($importOptions->useTimestamp()) {
            $columnsSet[] = sprintf(
                '%s = %s',
                TeradataQuote::quoteSingleIdentifier(ToStageImporterInterface::TIMESTAMP_COLUMN_NAME),
                TeradataQuote::quote($timestamp)
            );
        }

        if (!$importOptions->isNullManipulationEnabled()) {
            $columnsComparisonSql = array_map(
                static function ($columnName) use ($dest) {
                    return sprintf(
                        '%s.%s <> "src".%s',
                        $dest,
                        TeradataQuote::quoteSingleIdentifier($columnName),
                        TeradataQuote::quoteSingleIdentifier($columnName)
                    );
                },
                $stagingTableDefinition->getColumnsNames()
            );
        } else {
            $columnsComparisonSql = array_map(
                static function ($columnName) use ($dest) {
                    return sprintf(
                        'COALESCE(CAST(%s.%s AS VARCHAR(%s)), \'\') <> COALESCE("src".%s, \'\')',
                        $dest,
                        TeradataQuote::quoteSingleIdentifier($columnName),
                        Teradata::DEFAULT_NON_LATIN_CHAR_LENGTH,
                        TeradataQuote::quoteSingleIdentifier($columnName)
                    );
                },
                $stagingTableDefinition->getColumnsNames()
            );
        }

        return sprintf(
            'UPDATE %s FROM (%s) "src" SET %s WHERE %s AND (%s)',
            $dest,
            $this->getDedupSql(
                $stagingTableDefinition,
                $stagingTableDefinition,
                $destinationDefinition->getPrimaryKeysNames()
            ),
            implode(', ', $columnsSet),
            $this->getPrimayKeyWhereConditions($destinationDefinition->getPrimaryKeysNames(), $importOptions, $dest),
            implode(' OR ', $columnsComparisonSql)
        );
    }

    /**
     * @param string[] $primaryKeys
     */
    private function getPrimayKeyWhereConditions(
        array $primaryKeys,
        TeradataImportOptions $importOptions,
        string $dest,
        string $alias = '"src"'
    ): string {
        $pkWhereSql = array_map(function (string $col) use ($importOptions, $dest, $alias) {
            $str = 'TRIM(%s.%s) = COALESCE(TRIM(%s.%s), \'\')';
            if (!$importOptions->isNullManipulationEnabled()) {
                $str = 'TRIM(%s.%s) = TRIM(%s.%s)';
            }
            return sprintf(
                $str,
                $dest,
                TeradataQuote::quoteSingleIdentifier($col),
                $alias,
                TeradataQuote::quoteSingleIdentifier($col)
            );
        }, $primaryKeys);

        return implode(' AND ', $pkWhereSql);
    }
}
