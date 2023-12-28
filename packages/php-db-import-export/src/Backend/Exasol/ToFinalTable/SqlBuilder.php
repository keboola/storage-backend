<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Exasol\ToFinalTable;

use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Exasol;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\TableBackendUtils\Column\Exasol\ExasolColumn;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;

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
        ExasolTableDefinition $stagingTableDefinition,
        ExasolTableDefinition $deduplicationTableDefinition,
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
            ExasolQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            ExasolQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName()),
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
            ExasolQuote::quoteSingleIdentifier($deduplicationTableDefinition->getSchemaName()),
            ExasolQuote::quoteSingleIdentifier($deduplicationTableDefinition->getTableName()),
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
            return $alias . ExasolQuote::quoteSingleIdentifier($columns);
        }, $columns));
    }

    public function getDeleteOldItemsCommand(
        ExasolTableDefinition $stagingTableDefinition,
        ExasolTableDefinition $destinationTableDefinition,
    ): string {
        $stagingTable = sprintf(
            '%s.%s',
            ExasolQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            ExasolQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName()),
        );

        $destinationTable = sprintf(
            '%s.%s',
            ExasolQuote::quoteSingleIdentifier($destinationTableDefinition->getSchemaName()),
            ExasolQuote::quoteSingleIdentifier($destinationTableDefinition->getTableName()),
        );

        return sprintf(
            'DELETE FROM %s WHERE EXISTS (SELECT * FROM %s WHERE %s)',
            $stagingTable,
            $destinationTable,
            $this->getPrimaryKeyWhereConditionsSubstitute(
                $destinationTableDefinition->getPrimaryKeysNames(),
                $stagingTable,
                $destinationTable,
            ),
        );
    }

    /**
     * @param string[] $primaryKeys
     */
    private function getPrimaryKeyWhereConditionsSubstitute(
        array $primaryKeys,
        string $sourceTable,
        string $destinationTable,
    ): string {
        $pkWhereSql = array_map(function (string $col) use ($sourceTable, $destinationTable) {
            return sprintf(
                'COALESCE(%s.%s, \'KBC_$#\') = COALESCE(%s.%s, \'KBC_$#\')',
                $destinationTable,
                ExasolQuote::quoteSingleIdentifier($col),
                $sourceTable,
                ExasolQuote::quoteSingleIdentifier($col),
            );
        }, $primaryKeys);

        return rtrim(implode(' AND ', $pkWhereSql) . ' ');
    }

    /**
     * @param string[] $primaryKeys
     */
    private function getPrimaryKeyWhereConditionsNull(
        array $primaryKeys,
        string $sourceTable,
        string $destinationTable,
    ): string {
        $pkWhereSql = array_map(function (string $col) use ($sourceTable, $destinationTable) {
            return sprintf(
                '(%s.%s = %s.%s OR %s.%s IS NULL AND %s.%s IS NULL)',
                $destinationTable,
                ExasolQuote::quoteSingleIdentifier($col),
                $sourceTable,
                ExasolQuote::quoteSingleIdentifier($col),
                $destinationTable,
                ExasolQuote::quoteSingleIdentifier($col),
                $sourceTable,
                ExasolQuote::quoteSingleIdentifier($col),
            );
        }, $primaryKeys);

        return rtrim(implode(' AND ', $pkWhereSql) . ' ');
    }

    public function getDropTableIfExistsCommand(
        string $schema,
        string $tableName,
    ): string {
        return sprintf(
            'DROP TABLE IF EXISTS %s.%s',
            ExasolQuote::quoteSingleIdentifier($schema),
            ExasolQuote::quoteSingleIdentifier($tableName),
        );
    }

    public function getInsertAllIntoTargetTableCommand(
        ExasolTableDefinition $sourceTableDefinition,
        ExasolTableDefinition $destinationTableDefinition,
        ExasolImportOptions $importOptions,
        string $timestamp,
    ): string {
        $destinationTable = sprintf(
            '%s.%s',
            ExasolQuote::quoteSingleIdentifier($destinationTableDefinition->getSchemaName()),
            ExasolQuote::quoteSingleIdentifier($destinationTableDefinition->getTableName()),
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

        /** @var ExasolColumn $columnDefinition */
        foreach ($sourceTableDefinition->getColumnsDefinitions() as $columnDefinition) {
            if (in_array($columnDefinition->getColumnName(), $importOptions->getConvertEmptyValuesToNull(), true)) {
                // use nullif only for string base type
                if ($columnDefinition->getColumnDefinition()->getBasetype() === BaseType::STRING) {
                    $columnsSetSql[] = sprintf(
                        'NULLIF(%s, \'\')',
                        ExasolQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                    );
                } else {
                    $columnsSetSql[] = ExasolQuote::quoteSingleIdentifier($columnDefinition->getColumnName());
                }
            } else {
                $columnsSetSql[] = sprintf(
                    'CAST(COALESCE(%s, \'\') AS %s) AS %s',
                    ExasolQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                    $columnDefinition->getColumnDefinition()->getTypeOnlySQLDefinition(),
                    ExasolQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                );
            }
        }

        if ($useTimestamp) {
            $columnsSetSql[] = ExasolQuote::quote($timestamp);
        }

        return sprintf(
            'INSERT INTO %s (%s) (SELECT %s FROM %s.%s AS %s)',
            $destinationTable,
            $this->getColumnsString($insColumns),
            implode(',', $columnsSetSql),
            ExasolQuote::quoteSingleIdentifier($sourceTableDefinition->getSchemaName()),
            ExasolQuote::quoteSingleIdentifier($sourceTableDefinition->getTableName()),
            ExasolQuote::quoteSingleIdentifier(self::SRC_ALIAS),
        );
    }

    public function getTruncateTableWithDeleteCommand(
        string $schema,
        string $tableName,
    ): string {
        return sprintf(
            'DELETE FROM %s.%s',
            ExasolQuote::quoteSingleIdentifier($schema),
            ExasolQuote::quoteSingleIdentifier($tableName),
        );
    }

    public function getUpdateWithPkCommandSubstitute(
        ExasolTableDefinition $stagingTableDefinition,
        ExasolTableDefinition $destinationDefinition,
        ExasolImportOptions $importOptions,
        string $timestamp,
    ): string {
        $dest = sprintf(
            '%s.%s',
            ExasolQuote::quoteSingleIdentifier($destinationDefinition->getSchemaName()),
            ExasolQuote::quoteSingleIdentifier($destinationDefinition->getTableName()),
        );

        $columnsSet = [];
        /** @var ExasolColumn $columnDefinition */
        foreach ($stagingTableDefinition->getColumnsDefinitions() as $columnDefinition) {
            if (in_array($columnDefinition->getColumnName(), $destinationDefinition->getPrimaryKeysNames(), true)) {
                // primary keys are not updated
                continue;
            }
            $columnsSet[] = sprintf(
                '%s = "src".%s',
                ExasolQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                ExasolQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
            );
        }

        if ($importOptions->useTimestamp()) {
            $columnsSet[] = sprintf(
                '%s = \'%s\'',
                ExasolQuote::quoteSingleIdentifier(ToStageImporterInterface::TIMESTAMP_COLUMN_NAME),
                $timestamp,
            );
        }

        /** @var ExasolColumn[] $columnsList */
        $columnsList = iterator_to_array($stagingTableDefinition->getColumnsDefinitions());
        // update only changed rows - mysql TIMESTAMP ON UPDATE behaviour simulation
        $columnsComparisonSql = array_map(
            function (ExasolColumn $columnDefinition) {
                return sprintf(
                    'COALESCE(CAST("dest".%s AS %s), \'KBC_$#\') != COALESCE("src".%s, \'KBC_$#\')',
                    ExasolQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                    $columnDefinition->getColumnDefinition()->getTypeOnlySQLDefinition(),
                    ExasolQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                );
            },
            $columnsList,
        );

        return sprintf(
            'UPDATE %s AS "dest" SET %s FROM (SELECT DISTINCT * FROM %s.%s) AS "src",%s AS "dest" WHERE %s AND (%s) ',
            $dest,
            implode(', ', $columnsSet),
            ExasolQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            ExasolQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName()),
            $dest,
            $this->getPrimaryKeyWhereConditionsSubstitute(
                $destinationDefinition->getPrimaryKeysNames(),
                '"src"',
                '"dest"',
            ),
            implode(' OR ', $columnsComparisonSql),
        );
    }


    public function getUpdateWithPkCommandNull(
        ExasolTableDefinition $stagingTableDefinition,
        ExasolTableDefinition $destinationDefinition,
        ExasolImportOptions $importOptions,
        string $timestamp,
    ): string {
        $dest = sprintf(
            '%s.%s',
            ExasolQuote::quoteSingleIdentifier($destinationDefinition->getSchemaName()),
            ExasolQuote::quoteSingleIdentifier($destinationDefinition->getTableName()),
        );

        $columnsSet = [];
        /** @var ExasolColumn $columnDefinition */
        foreach ($stagingTableDefinition->getColumnsDefinitions() as $columnDefinition) {
            if (in_array($columnDefinition->getColumnName(), $destinationDefinition->getPrimaryKeysNames(), true)) {
                // primary keys are not updated
                continue;
            }
            $columnsSet[] = sprintf(
                '%s = "src".%s',
                ExasolQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                ExasolQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
            );
        }

        if ($importOptions->useTimestamp()) {
            $columnsSet[] = sprintf(
                '%s = \'%s\'',
                ExasolQuote::quoteSingleIdentifier(ToStageImporterInterface::TIMESTAMP_COLUMN_NAME),
                $timestamp,
            );
        }

        /** @var ExasolColumn[] $columnsList */
        $columnsList = iterator_to_array($stagingTableDefinition->getColumnsDefinitions());
        // update only changed rows - mysql TIMESTAMP ON UPDATE behaviour simulation
        $columnsComparisonSql = array_map(
            function (ExasolColumn $columnDefinition) {
                return sprintf(
                // phpcs:ignore
                    '("dest".%s != "src".%s OR ("dest".%s IS NULL OR "src".%s IS NULL AND ("dest".%s IS NULL AND "src".%s IS NULL)))',
                    ExasolQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                    ExasolQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                    ExasolQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                    ExasolQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                    ExasolQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                    ExasolQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                );
            },
            $columnsList,
        );

        return sprintf(
            'UPDATE %s AS "dest" SET %s FROM (SELECT DISTINCT * FROM %s.%s) AS "src",%s AS "dest" WHERE %s AND (%s) ',
            $dest,
            implode(', ', $columnsSet),
            ExasolQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            ExasolQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName()),
            $dest,
            $this->getPrimaryKeyWhereConditionsNull(
                $destinationDefinition->getPrimaryKeysNames(),
                '"src"',
                '"dest"',
            ),
            implode(' OR ', $columnsComparisonSql),
        );
    }
}
