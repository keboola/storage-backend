<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse;

use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\Synapse\Table;

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

        $pkSql = $this->getColumnsString(
            $primaryKeys,
            ','
        );

        $depudeSql = sprintf(
            'SELECT %s FROM ('
            . 'SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) AS "_row_number_" '
            . 'FROM %s.%s'
            . ') AS a '
            . 'WHERE a."_row_number_" = 1',
            $this->getColumnsString($source->getColumnsNames(), ',', 'a'),
            $this->getColumnsString($source->getColumnsNames(), ', '),
            $pkSql,
            $pkSql,
            $this->platform->quoteSingleIdentifier($destination->getSchema()),
            $this->platform->quoteSingleIdentifier($stagingTableName)
        );

        return sprintf(
            'INSERT INTO %s.%s (%s) %s',
            $this->platform->quoteSingleIdentifier($destination->getSchema()),
            $this->platform->quoteSingleIdentifier($tempTableName),
            $this->getColumnsString($source->getColumnsNames()),
            $depudeSql
        );
    }

    public function getCtasDedupCommand(
        SourceInterface $source,
        Table $destination,
        string $stagingTableName,
        ImportOptionsInterface $importOptions,
        string $timestamp,
        DestinationTableOptions $destinationTableOptions,
        bool $skipCasting = false
    ): string {
        if (empty($destinationTableOptions->getPrimaryKeys())) {
            return '';
        }
        $distributionSql = $this->getSqlDistributionPart($destinationTableOptions);

        $pkSql = $this->getColumnsString(
            $destinationTableOptions->getPrimaryKeys(),
            ','
        );

        $columnsInOrder = $destinationTableOptions->getColumnNamesInOrder();
        $timestampColIndex = array_search(
            Importer::TIMESTAMP_COLUMN_NAME,
            $columnsInOrder,
            true
        );
        if ($timestampColIndex !== false) {
            // remove timestamp column if exists in ordered columns
            unset($columnsInOrder[$timestampColIndex]);
        }

        $useTimestamp = !in_array(Importer::TIMESTAMP_COLUMN_NAME, $source->getColumnsNames(), true)
            && $importOptions->useTimestamp();

        $createTableColumns = $this->getColumnsString($columnsInOrder, ',', 'a');
        if ($useTimestamp) {
            $createTableColumns .= ', ' . $this->connection->quote($timestamp) . ' AS [_timestamp]';
        }

        $columnsSetSql = [];
        foreach ($source->getColumnsNames() as $columnName) {
            if (in_array($columnName, $importOptions->getConvertEmptyValuesToNull(), true)) {
                $colSql = sprintf(
                    'NULLIF(%s, \'\')',
                    $this->platform->quoteSingleIdentifier($columnName)
                );
                if ($skipCasting === false) {
                    $colSql = sprintf(
                        'CAST(%s as nvarchar(4000))',
                        $colSql
                    );
                }
                $columnsSetSql[] = sprintf(
                    '%s AS %s',
                    $colSql,
                    $this->platform->quoteSingleIdentifier($columnName)
                );
            } else {
                $colSql = sprintf(
                    'COALESCE(%s, \'\')',
                    $this->platform->quoteSingleIdentifier($columnName)
                );
                if ($skipCasting === false) {
                    $colSql = sprintf(
                        'CAST(%s as nvarchar(4000))',
                        $colSql
                    );
                }
                $columnsSetSql[] = sprintf(
                    '%s AS %s',
                    $colSql,
                    $this->platform->quoteSingleIdentifier($columnName)
                );
            }
        }

        $depudeSql = sprintf(
            'SELECT %s FROM ('
            . 'SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) AS "_row_number_" '
            . 'FROM %s.%s'
            . ') AS a '
            . 'WHERE a."_row_number_" = 1',
            $createTableColumns,
            implode(',', $columnsSetSql),
            $pkSql,
            $pkSql,
            $this->platform->quoteSingleIdentifier($destination->getSchema()),
            $this->platform->quoteSingleIdentifier($stagingTableName)
        );

        return sprintf(
            'CREATE TABLE %s WITH (%s) AS %s',
            $destination->getQuotedTableWithScheme(),
            $distributionSql,
            $depudeSql
        );
    }

    public function getColumnsString(
        array $columns,
        string $delimiter = ', ',
        ?string $tableAlias = null
    ): string {
        return implode($delimiter, array_map(function ($columns) use (
            $tableAlias
        ) {
            $alias = $tableAlias === null ? '' : $tableAlias . '.';
            return $alias . $this->platform->quoteSingleIdentifier($columns);
        }, $columns));
    }

    public function getDeleteOldItemsCommand(
        Table $destination,
        string $stagingTableName,
        array $primaryKeys
    ): string {
        $stagingTable = sprintf(
            '%s.%s',
            $this->platform->quoteSingleIdentifier($destination->getSchema()),
            $this->platform->quoteSingleIdentifier($stagingTableName)
        );

        return sprintf(
            'DELETE %s WHERE EXISTS (SELECT * FROM %s WHERE %s)',
            $stagingTable,
            $destination->getQuotedTableWithScheme(),
            $this->getPrimaryKeyWhereConditions(
                $primaryKeys,
                $stagingTable,
                $destination->getQuotedTableWithScheme()
            )
        );
    }

    private function getPrimaryKeyWhereConditions(
        array $primaryKeys,
        string $sourceTable,
        string $destinationTable
    ): string {
        $pkWhereSql = array_map(function (string $col) use ($sourceTable, $destinationTable) {
            return sprintf(
                '%s.%s = COALESCE(%s.%s, \'\')',
                $destinationTable,
                $this->platform->quoteSingleIdentifier($col),
                $sourceTable,
                $this->platform->quoteSingleIdentifier($col)
            );
        }, $primaryKeys);

        return rtrim(implode(' AND ', $pkWhereSql) . ' ');
    }

    public function getDropCommand(
        string $schema,
        string $tableName
    ): string {
        return sprintf(
            'DROP TABLE %s.%s',
            $this->platform->quoteSingleIdentifier($schema),
            $this->platform->quoteSingleIdentifier($tableName)
        );
    }

    public function getDropTableIfExistsCommand(
        string $schema,
        string $tableName
    ): string {
        $table = sprintf(
            '%s.%s',
            $this->platform->quoteSingleIdentifier($schema),
            $this->platform->quoteSingleIdentifier($tableName)
        );
        return sprintf(
            'IF OBJECT_ID (N\'%s\', N\'U\') IS NOT NULL DROP TABLE %s',
            $table,
            $table
        );
    }

    public function getInsertAllIntoTargetTableCommand(
        SourceInterface $source,
        Table $destination,
        ImportOptionsInterface $importOptions,
        string $stagingTableName,
        string $timestamp
    ): string {
        $insColumns = $source->getColumnsNames();
        $useTimestamp = !in_array(Importer::TIMESTAMP_COLUMN_NAME, $insColumns, true)
            && $importOptions->useTimestamp();

        if ($useTimestamp) {
            $insColumns = array_merge($source->getColumnsNames(), [Importer::TIMESTAMP_COLUMN_NAME]);
        }

        $columnsSetSql = [];

        foreach ($source->getColumnsNames() as $columnName) {
            if (in_array($columnName, $importOptions->getConvertEmptyValuesToNull(), true)) {
                $columnsSetSql[] = sprintf(
                    'NULLIF(%s, \'\')',
                    $this->platform->quoteSingleIdentifier($columnName)
                );
            } else {
                $columnsSetSql[] = sprintf(
                    'CAST(COALESCE(%s, \'\') as nvarchar(4000)) AS %s',
                    $this->platform->quoteSingleIdentifier($columnName),
                    $this->platform->quoteSingleIdentifier($columnName)
                );
            }
        }

        if ($useTimestamp) {
            $columnsSetSql[] = $this->connection->quote($timestamp);
        }

        return sprintf(
            'INSERT INTO %s (%s) (SELECT %s FROM %s.%s AS [src])',
            $destination->getQuotedTableWithScheme(),
            $this->getColumnsString($insColumns),
            implode(',', $columnsSetSql),
            $this->platform->quoteSingleIdentifier($destination->getSchema()),
            $this->platform->quoteSingleIdentifier($stagingTableName)
        );
    }

    public function getRenameTableCommand(
        string $schema,
        string $sourceTableName,
        string $targetTable
    ): string {
        return sprintf(
            'RENAME OBJECT %s.%s TO %s',
            $this->platform->quoteSingleIdentifier($schema),
            $this->platform->quoteSingleIdentifier($sourceTableName),
            $this->platform->quoteSingleIdentifier($targetTable)
        );
    }

    private function getSqlDistributionPart(DestinationTableOptions $destinationTableOptions): string
    {
        $distributionSql = sprintf(
            'DISTRIBUTION=%s',
            $destinationTableOptions->getDistribution()->getDistributionName()
        );

        if ($destinationTableOptions->getDistribution()->isHashDistribution()) {
            $distributionSql = sprintf(
                '%s(%s)',
                $distributionSql,
                $this->platform->quoteSingleIdentifier(
                    $destinationTableOptions->getDistribution()->getDistributionColumnsNames()[0]
                )
            );
        }
        return $distributionSql;
    }

    public function getTruncateTableWithDeleteCommand(
        string $schema,
        string $tableName
    ): string {
        return sprintf(
            'DELETE FROM %s.%s',
            $this->platform->quoteSingleIdentifier($schema),
            $this->platform->quoteSingleIdentifier($tableName)
        );
    }

    public function getUpdateWithPkCommand(
        SourceInterface $source,
        Table $destination,
        ImportOptionsInterface $importOptions,
        string $stagingTableName,
        array $primaryKeys,
        string $timestamp
    ): string {
        $dest = $destination->getQuotedTableWithScheme();
        $columnsSet = [];
        foreach ($source->getColumnsNames() as $columnName) {
            if (in_array($columnName, $primaryKeys, true)) {
                // primary keys are not updated
                continue;
            }
            if (in_array($columnName, $importOptions->getConvertEmptyValuesToNull(), true)) {
                $columnsSet[] = sprintf(
                    '%s = NULLIF([src].%s, \'\')',
                    $this->platform->quoteSingleIdentifier($columnName),
                    $this->platform->quoteSingleIdentifier($columnName)
                );
            } else {
                $columnsSet[] = sprintf(
                    '%s = COALESCE([src].%s, \'\')',
                    $this->platform->quoteSingleIdentifier($columnName),
                    $this->platform->quoteSingleIdentifier($columnName)
                );
            }
        }

        if ($importOptions->useTimestamp()) {
            $columnsSet[] = sprintf(
                '%s = \'%s\'',
                $this->platform->quoteSingleIdentifier(Importer::TIMESTAMP_COLUMN_NAME),
                $timestamp
            );
        }

        // update only changed rows - mysql TIMESTAMP ON UPDATE behaviour simulation
        $columnsComparisionSql = array_map(
            function ($columnName) use ($dest) {
                return sprintf(
                    'COALESCE(CAST(%s.%s AS varchar(4000)), \'\') != COALESCE([src].%s, \'\')',
                    $dest,
                    $this->platform->quoteSingleIdentifier($columnName),
                    $this->platform->quoteSingleIdentifier($columnName)
                );
            },
            $source->getColumnsNames()
        );

        return sprintf(
            'UPDATE %s SET %s FROM %s.%s AS [src] WHERE %s AND (%s) ',
            $dest,
            implode(', ', $columnsSet),
            $this->platform->quoteSingleIdentifier($destination->getSchema()),
            $this->platform->quoteSingleIdentifier($stagingTableName),
            $this->getPrimaryKeyWhereConditions($primaryKeys, '[src]', $dest),
            implode(' OR ', $columnsComparisionSql)
        );
    }
}
