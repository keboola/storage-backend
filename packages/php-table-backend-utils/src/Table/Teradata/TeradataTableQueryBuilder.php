<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Teradata;

use Keboola\Datatype\Definition\Teradata;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\QueryBuilderException;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\TableQueryBuilderInterface;

class TeradataTableQueryBuilder implements TableQueryBuilderInterface
{
//https://docs.teradata.com/r/eWpPpcMoLGQcZEoyt5AjEg/IEGchL9GChJgIJTiksS7tQ
    public const DISALLOWED_PK_TYPES = [
        Teradata::TYPE_BLOB,
        Teradata::TYPE_CLOB,
//        TODO there are more disallowed types but they are not implemented yet in phpdatatypes
    ];

    private const INVALID_PKS_FOR_TABLE = 'invalidPKs';

    public function getCreateTempTableCommand(string $schemaName, string $tableName, ColumnCollection $columns): string
    {
        // TODO: Implement getCreateTempTableCommand() method.
        throw new \Exception('method is not implemented yet');
    }

    public function getDropTableCommand(string $schemaName, string $tableName): string
    {
        return sprintf(
            'DROP TABLE %s.%s',
            TeradataQuote::quoteSingleIdentifier($schemaName),
            TeradataQuote::quoteSingleIdentifier($tableName)
        );
    }

    public function getRenameTableCommand(string $dbName, string $sourceTableName, string $newTableName): string
    {
        $quotedDbName = TeradataQuote::quoteSingleIdentifier($dbName);
        return sprintf(
            'RENAME TABLE %s.%s AS %s.%s',
            $quotedDbName,
            TeradataQuote::quoteSingleIdentifier($sourceTableName),
            $quotedDbName,
            TeradataQuote::quoteSingleIdentifier($newTableName)
        );
    }

    public function getTruncateTableCommand(string $schemaName, string $tableName): string
    {
        return sprintf(
            'DELETE %s.%s ALL',
            TeradataQuote::quoteSingleIdentifier($schemaName),
            TeradataQuote::quoteSingleIdentifier($tableName)
        );
    }

    /**
     * @inheritDoc
     */
    public function getCreateTableCommand(
        string $schemaName,
        string $tableName,
        ColumnCollection $columns,
        array $primaryKeys = []
    ): string {
        $columnNames = [];
        $columnsSqlDefinitions = [];
        /** @var TeradataColumn $column */
        foreach ($columns->getIterator() as $column) {
            $columnName = $column->getColumnName();
            $columnNames[] = $columnName;
            /** @var Teradata $columnDefinition */
            $columnDefinition = $column->getColumnDefinition();
            $columnsSqlDefinitions[] = sprintf(
                '%s %s',
                TeradataQuote::quoteSingleIdentifier($columnName),
                $columnDefinition->getSQLDefinition()
            );

            // check if PK can be defined on selected columns
            if (in_array($columnName, $primaryKeys, true)) {
                $columnType = $columnDefinition->getType();

                if (in_array($columnType, self::DISALLOWED_PK_TYPES, true)) {
                    throw new QueryBuilderException(
                        sprintf(
                            'Trying to set PK on column %s but type %s is not supported for PK',
                            $columnName,
                            $columnType
                        ),
                        self::INVALID_PKS_FOR_TABLE
                    );
                }

                if ($columnDefinition->isNullable()) {
                    throw new QueryBuilderException(
                        sprintf('Trying to set PK on column %s but this column is nullable', $columnName),
                        self::INVALID_PKS_FOR_TABLE
                    );
                }
            }
        }

        // check that all PKs are valid columns
        $pksNotPresentInColumns = array_diff($primaryKeys, $columnNames);
        if ($pksNotPresentInColumns !== []) {
            throw new QueryBuilderException(
                sprintf(
                    'Trying to set %s as PKs but not present in columns',
                    implode(',', $pksNotPresentInColumns)
                ),
                self::INVALID_PKS_FOR_TABLE
            );
        }

        $columnsSql = implode(",\n", $columnsSqlDefinitions);

        if ($primaryKeys) {
            $columnsSql .= sprintf(
                ",\nPRIMARY KEY (%s)",
                implode(
                    ', ',
                    array_map(static function ($item) {
                        return TeradataQuote::quoteSingleIdentifier($item);
                    }, $primaryKeys)
                )
            );
        }

        // TODO add settings to TABLE such as JOURNAL etc...
        return sprintf(
            'CREATE MULTISET TABLE %s.%s, FALLBACK
(%s) %s;',
            TeradataQuote::quoteSingleIdentifier($schemaName),
            TeradataQuote::quoteSingleIdentifier($tableName),
            $columnsSql,
            // NoPI table support duplications in table
            $primaryKeys ? '' : ' NO PRIMARY INDEX'
        );
    }

    public function getCreateTableCommandFromDefinition(
        TableDefinitionInterface $definition,
        bool $definePrimaryKeys = self::CREATE_TABLE_WITHOUT_PRIMARY_KEYS
    ): string {
        throw new \Exception('method is not implemented yet');
    }
}
