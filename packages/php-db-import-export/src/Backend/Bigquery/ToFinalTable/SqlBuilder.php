<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable;

use Exception as InternalException;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;

class SqlBuilder
{
    private const SRC_ALIAS = 'src';

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
        return 'ROLLBACK TRANSACTION';
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
            BigqueryQuote::quoteSingleIdentifier($tableName)
        );
    }

    public function getDropTableIfExistsCommand(
        string $schema,
        string $tableName
    ): string {
        return sprintf(
            'DROP TABLE IF EXISTS %s.%s',
            BigqueryQuote::quoteSingleIdentifier($schema),
            BigqueryQuote::quoteSingleIdentifier($tableName)
        );
    }

    public function getTableExistsCommand(string $dbName, string $tableName): string
    {
        return sprintf(
            'SELECT COUNT(*) AS count FROM %s.INFORMATION_SCHEMA.TABLES WHERE table_name = %s;',
            BigqueryQuote::quoteSingleIdentifier($dbName),
            BigqueryQuote::quote($tableName)
        );
    }

    public function getInsertAllIntoTargetTableCommand(
        BigqueryTableDefinition $sourceTableDefinition,
        BigqueryTableDefinition $destinationTableDefinition,
        ImportOptions $importOptions,
        string $timestamp
    ): string {
        $destinationTable = sprintf(
            '%s.%s',
            BigqueryQuote::quoteSingleIdentifier($destinationTableDefinition->getSchemaName()),
            BigqueryQuote::quoteSingleIdentifier($destinationTableDefinition->getTableName())
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

        /** @var BigqueryColumn $columnDefinition */
        foreach ($sourceTableDefinition->getColumnsDefinitions() as $columnDefinition) {
            $destinationColumn = null;
            // case sensitive search
            /** @var BigqueryColumn $col */
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
                        BigqueryQuote::quoteSingleIdentifier($columnDefinition->getColumnName())
                    );
                } else {
                    $columnsSetSql[] = BigqueryQuote::quoteSingleIdentifier($columnDefinition->getColumnName());
                }
            } elseif ($columnDefinition->getColumnDefinition()->getBasetype() === BaseType::STRING) {
                $columnsSetSql[] = sprintf(
                    'CAST(COALESCE(%s, \'\') as %s) AS %s',
                    BigqueryQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                    $destinationColumn->getColumnDefinition()->getType(),
                    BigqueryQuote::quoteSingleIdentifier($columnDefinition->getColumnName())
                );
            } else {
                // on columns other than string dont use COALESCE, use direct cast
                // this will fail if the column is not null, but this is expected
                $columnsSetSql[] = sprintf(
                    'CAST(%s as %s) AS %s',
                    BigqueryQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
                    $destinationColumn->getColumnDefinition()->getType(),
                    BigqueryQuote::quoteSingleIdentifier($columnDefinition->getColumnName())
                );
            }
        }

        if ($useTimestamp) {
            $columnsSetSql[] = sprintf('CAST(%s as %s)', BigqueryQuote::quote($timestamp), Bigquery::TYPE_TIMESTAMP);
        }

        return sprintf(
            'INSERT INTO %s (%s) SELECT %s FROM %s.%s AS %s',
            $destinationTable,
            $this->getColumnsString($columnsToInsert),
            implode(',', $columnsSetSql),
            BigqueryQuote::quoteSingleIdentifier($sourceTableDefinition->getSchemaName()),
            BigqueryQuote::quoteSingleIdentifier($sourceTableDefinition->getTableName()),
            BigqueryQuote::quoteSingleIdentifier(self::SRC_ALIAS)
        );
    }

    public function getTruncateTableWithDeleteCommand(
        string $schema,
        string $tableName
    ): string {
        return sprintf(
            'TRUNCATE TABLE %s.%s',
            BigqueryQuote::quoteSingleIdentifier($schema),
            BigqueryQuote::quoteSingleIdentifier($tableName)
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
            return $alias . BigqueryQuote::quoteSingleIdentifier($columns);
        }, $columns));
    }

    public function getDeleteOldItemsCommand(
        BigqueryTableDefinition $stagingTableDefinition,
        BigqueryTableDefinition $destinationTableDefinition
    ): void {
        throw new InternalException('not implemented yet');
    }
}
