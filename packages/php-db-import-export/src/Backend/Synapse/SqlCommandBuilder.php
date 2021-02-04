<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Exception;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\Synapse\Table;

class SqlCommandBuilder
{
    /** @var Connection */
    private $connection;

    /** @var SQLServer2012Platform|AbstractPlatform */
    private $platform;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        $this->platform = $connection->getDatabasePlatform();
    }

    public function getBeginTransaction(): string
    {
        return 'BEGIN TRANSACTION';
    }

    public function getCommitTransaction(): string
    {
        return 'COMMIT';
    }

    public function getCopyTableCommand(
        string $schema,
        string $sourceTable,
        string $destinationTable
    ): string {
        return sprintf(
            'INSERT INTO %s.%s SELECT * FROM %s.%s',
            $this->platform->quoteSingleIdentifier($schema),
            $this->platform->quoteSingleIdentifier($destinationTable),
            $this->platform->quoteSingleIdentifier($schema),
            $this->platform->quoteSingleIdentifier($sourceTable)
        );
    }

    public function getCreateTempTableCommand(
        string $schema,
        string $tableName,
        array $columns,
        SynapseImportOptions $options
    ): string {
        $this->assertStagingTable($tableName);

        switch ($options->getTempTableType()) {
            case SynapseImportOptions::TEMP_TABLE_HEAP:
                $columnsSql = array_map(function ($column) {
                    return sprintf('%s nvarchar(max)', $this->platform->quoteSingleIdentifier($column));
                }, $columns);
                return sprintf(
                    'CREATE TABLE %s.%s (%s) WITH (HEAP, LOCATION = USER_DB)',
                    $this->platform->quoteSingleIdentifier($schema),
                    $this->platform->quoteSingleIdentifier($tableName),
                    implode(', ', $columnsSql)
                );
            case SynapseImportOptions::TEMP_TABLE_HEAP_4000:
                $columnsSql = array_map(function ($column) {
                    return sprintf('%s nvarchar(4000)', $this->platform->quoteSingleIdentifier($column));
                }, $columns);
                return sprintf(
                    'CREATE TABLE %s.%s (%s) WITH (HEAP, LOCATION = USER_DB)',
                    $this->platform->quoteSingleIdentifier($schema),
                    $this->platform->quoteSingleIdentifier($tableName),
                    implode(', ', $columnsSql)
                );
            case SynapseImportOptions::TEMP_TABLE_COLUMNSTORE:
                $columnsSql = array_map(function ($column) {
                    return sprintf('%s nvarchar(4000)', $this->platform->quoteSingleIdentifier($column));
                }, $columns);
                return sprintf(
                    'CREATE TABLE %s.%s (%s) WITH (CLUSTERED COLUMNSTORE INDEX)',
                    $this->platform->quoteSingleIdentifier($schema),
                    $this->platform->quoteSingleIdentifier($tableName),
                    implode(', ', $columnsSql)
                );
            case SynapseImportOptions::TEMP_TABLE_CLUSTERED_INDEX:
                $columnsSql = array_map(function ($column) {
                    return sprintf('%s nvarchar(4000)', $this->platform->quoteSingleIdentifier($column));
                }, $columns);
                $columnsIndex = array_map(function ($column) {
                    return sprintf('%s', $this->platform->quoteSingleIdentifier($column));
                }, $columns);
                return sprintf(
                    'CREATE TABLE %s.%s (%s) WITH (CLUSTERED INDEX(%s))',
                    $this->platform->quoteSingleIdentifier($schema),
                    $this->platform->quoteSingleIdentifier($tableName),
                    implode(', ', $columnsSql),
                    implode(', ', $columnsIndex)
                );
        }

        throw new \LogicException(sprintf('Unknown temp table type "%s".', $options->getTempTableType()));
    }

    private function assertStagingTable(string $tableName): void
    {
        if ($tableName[0] !== '#') {
            throw new Exception(sprintf(
                'Staging table must start with "#" table name "%s" supplied',
                $tableName
            ));
        }
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
        array $primaryKeys,
        string $stagingTableName,
        ImportOptionsInterface $importOptions,
        string $timestamp,
        array $columnsInOrder
    ): string {
        if (empty($primaryKeys)) {
            return '';
        }

        $pkSql = $this->getColumnsString(
            $primaryKeys,
            ','
        );

        $timestampColIndex = array_search(Importer::TIMESTAMP_COLUMN_NAME, $columnsInOrder, true);
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
                $columnsSetSql[] = sprintf(
                    'CAST(NULLIF(%s, \'\') as nvarchar(4000)) AS %s',
                    $this->platform->quoteSingleIdentifier($columnName),
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
            'CREATE TABLE %s WITH (DISTRIBUTION = ROUND_ROBIN) AS %s',
            $destination->getQuotedTableWithScheme(),
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

    public function getTableObjectIdCommand(string $schemaName, string $tableName): string
    {
        return sprintf(
            'SELECT [object_id] FROM sys.tables WHERE schema_name(schema_id) = %s AND NAME = %s',
            $this->connection->quote($schemaName),
            $this->connection->quote($tableName)
        );
    }

    public function getTableItemsCountCommand(string $schema, string $table): string
    {
        return sprintf(
            'SELECT COUNT(*) AS [count] FROM %s.%s',
            $this->platform->quoteSingleIdentifier($schema),
            $this->platform->quoteSingleIdentifier($table)
        );
    }

    public function getTablePrimaryKey(string $schemaName, string $tableName): array
    {
        /** @var string|false $objectId */
        $objectId = $this->connection->fetchColumn(
            $this->getTableObjectIdCommand($schemaName, $tableName)
        );

        if ($objectId === false) {
            throw new Exception(sprintf('Table %s.%s does not exist.', $schemaName, $tableName));
        }

        $result = $this->connection->fetchAll(
            <<< EOT
SELECT COL_NAME(ic.OBJECT_ID,ic.column_id) AS ColumnName
    FROM sys.indexes AS i INNER JOIN
        sys.index_columns AS ic ON i.OBJECT_ID = ic.OBJECT_ID AND i.index_id = ic.index_id
    WHERE i.is_primary_key = 1 AND i.OBJECT_ID = '$objectId'
EOT
        );

        return array_map(static function ($item) {
            return $item['ColumnName'];
        }, $result);
    }

    public function getTruncateTableCommand(
        string $schema,
        string $tableName
    ): string {
        return sprintf(
            'TRUNCATE TABLE %s.%s',
            $this->platform->quoteSingleIdentifier($schema),
            $this->platform->quoteSingleIdentifier($tableName)
        );
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
                    'COALESCE(CAST(%s.%s AS varchar), \'\') != COALESCE([src].%s, \'\')',
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
