<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Teradata;

use Exception;
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
    private const PK_CONSTRAINT_NAME = 'kbc_pk';

    public function getCreateTempTableCommand(string $schemaName, string $tableName, ColumnCollection $columns): string
    {
        // TODO: Implement getCreateTempTableCommand() method.
        throw new Exception('method is not implemented yet');
    }

    public function getDropTableCommand(string $schemaName, string $tableName): string
    {
        return sprintf(
            'DROP TABLE %s.%s',
            TeradataQuote::quoteSingleIdentifier($schemaName),
            TeradataQuote::quoteSingleIdentifier($tableName),
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
            TeradataQuote::quoteSingleIdentifier($newTableName),
        );
    }

    public function getTruncateTableCommand(string $schemaName, string $tableName): string
    {
        return sprintf(
            'DELETE %s.%s ALL',
            TeradataQuote::quoteSingleIdentifier($schemaName),
            TeradataQuote::quoteSingleIdentifier($tableName),
        );
    }

    public function getAddColumnCommand(string $schemaName, string $tableName, TeradataColumn $columnDefinition): string
    {
        return sprintf(
            'ALTER TABLE %s.%s ADD %s %s',
            TeradataQuote::quoteSingleIdentifier($schemaName),
            TeradataQuote::quoteSingleIdentifier($tableName),
            TeradataQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
            $columnDefinition->getColumnDefinition()->getSQLDefinition(),
        );
    }

    public function getDropColumnCommand(string $schemaName, string $tableName, string $columnName): string
    {
        return sprintf(
            'ALTER TABLE %s.%s DROP %s',
            TeradataQuote::quoteSingleIdentifier($schemaName),
            TeradataQuote::quoteSingleIdentifier($tableName),
            TeradataQuote::quoteSingleIdentifier($columnName),
        );
    }

    /**
     * @inheritDoc
     */
    public function getCreateTableCommand(
        string $schemaName,
        string $tableName,
        ColumnCollection $columns,
        array $primaryKeys = [],
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
                $columnDefinition->getSQLDefinition(),
            );

            // check if PK can be defined on selected columns
            if (in_array($columnName, $primaryKeys, true)) {
                $columnType = $columnDefinition->getType();

                if (in_array($columnType, self::DISALLOWED_PK_TYPES, true)) {
                    throw new QueryBuilderException(
                        sprintf(
                            'Trying to set PK on column %s but type %s is not supported for PK',
                            $columnName,
                            $columnType,
                        ),
                        self::INVALID_PKS_FOR_TABLE,
                    );
                }

                if ($columnDefinition->isNullable()) {
                    throw new QueryBuilderException(
                        sprintf('Trying to set PK on column %s but this column is nullable', $columnName),
                        self::INVALID_PKS_FOR_TABLE,
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
                    implode(',', $pksNotPresentInColumns),
                ),
                self::INVALID_PKS_FOR_TABLE,
            );
        }

        $columnsSql = implode(",\n", $columnsSqlDefinitions);

        if ($primaryKeys !== []) {
            $columnsSql .= sprintf(
                ",\nCONSTRAINT %s PRIMARY KEY (%s)",
                self::PK_CONSTRAINT_NAME,
                implode(
                    ', ',
                    array_map(static fn($item) => TeradataQuote::quoteSingleIdentifier($item), $primaryKeys),
                ),
            );
        }

        // TODO add settings to TABLE such as JOURNAL etc...
        return sprintf(
            'CREATE MULTISET TABLE %s.%s, FALLBACK
(%s)%s;',
            TeradataQuote::quoteSingleIdentifier($schemaName),
            TeradataQuote::quoteSingleIdentifier($tableName),
            $columnsSql,
            // NoPI table support duplications in table
            $primaryKeys !== [] ? '' : ' NO PRIMARY INDEX',
        );
    }

    public function getCreateTableCommandFromDefinition(
        TableDefinitionInterface $definition,
        bool $definePrimaryKeys = self::CREATE_TABLE_WITHOUT_PRIMARY_KEYS,
    ): string {
        assert($definition instanceof TeradataTableDefinition);
        return $this->getCreateTableCommand(
            $definition->getSchemaName(),
            $definition->getTableName(),
            $definition->getColumnsDefinitions(),
            $definePrimaryKeys === self::CREATE_TABLE_WITH_PRIMARY_KEYS
                ? $definition->getPrimaryKeysNames()
                : [],
        );
    }

    /**
     * @param string[] $columns
     */
    public function getAddPrimaryKeyCommand(string $schemaName, string $tableName, array $columns): string
    {
        return sprintf(
            'ALTER TABLE %s.%s ADD CONSTRAINT %s PRIMARY KEY (%s);',
            TeradataQuote::quoteSingleIdentifier($schemaName),
            TeradataQuote::quoteSingleIdentifier($tableName),
            self::PK_CONSTRAINT_NAME,
            implode(',', array_map(fn($item) => TeradataQuote::quoteSingleIdentifier($item), $columns)),
        );
    }

    public function getDropPrimaryKeyCommand(string $schemaName, string $tableName): string
    {
        return sprintf(
            'ALTER TABLE %s.%s DROP CONSTRAINT %s;',
            TeradataQuote::quoteSingleIdentifier($schemaName),
            TeradataQuote::quoteSingleIdentifier($tableName),
            self::PK_CONSTRAINT_NAME,
        );
    }

    /**
     * @param string[] $columns
     */
    public function getCommandForDuplicates(string $schemaName, string $tableName, array $columns): string
    {
        $formattedColumns = implode(
            ',',
            array_map(fn($item) => TeradataQuote::quoteSingleIdentifier($item), $columns),
        );
        return sprintf(
            <<<SQL
SELECT MAX("_row_number_") AS "count" FROM
(
    SELECT ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) AS "_row_number_" FROM %s.%s
) "data"
SQL
            ,
            $formattedColumns,
            $formattedColumns,
            TeradataQuote::quoteSingleIdentifier($schemaName),
            TeradataQuote::quoteSingleIdentifier($tableName),
        );
    }
}
