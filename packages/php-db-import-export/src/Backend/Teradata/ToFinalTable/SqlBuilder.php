<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable;

use Keboola\Datatype\Definition\BaseType;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;

class SqlBuilder
{
    const SRC_ALIAS = 'src';

    public function getCommitTransaction(): string
    {
        //TODO
        throw new \Exception('not implemented yet');
    }

    /**
     * SQL to drop table. DOES NOT check existence of table
     *
     * @param string $dbName
     * @param string $tableName
     * @return string
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
            'SELECT COUNT(*) FROM DBC.Tables WHERE DatabaseName = %s AND TableName = %s;',
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
            } else {
                $columnsSetSql[] = sprintf(
                    'CAST(COALESCE(%s, \'\') AS %s) AS %s',
                    TeradataQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                    $columnDefinition->getColumnDefinition()->getSQLDefinition(),
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

    public function getTruncateTableWithDeleteCommand(
        string $schema,
        string $tableName
    ): string {
        return sprintf(
            'DELETE FROM %s.%s',
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

    public function getDeleteOldItemsCommand()
    {
        throw new \Exception('not implemented yet');
    }
}
