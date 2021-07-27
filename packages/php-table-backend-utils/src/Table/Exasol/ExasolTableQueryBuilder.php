<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Exasol;

use Keboola\Datatype\Definition\Exasol;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Exasol\ExasolColumn;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\QueryBuilderException;
use Keboola\TableBackendUtils\Table\TableQueryBuilderInterface;

class ExasolTableQueryBuilder implements TableQueryBuilderInterface
{
//https://docs.Exasol.com/r/eWpPpcMoLGQcZEoyt5AjEg/IEGchL9GChJgIJTiksS7tQ
    public const DISALLOWED_PK_TYPES = [
//        Exasol::TYPE_BLOB,
        Exasol::TYPE_CLOB,
//        TODO there are more disallowed types but they are not implemented yet in phpdatatypes
    ];

    private const INVALID_PKS_FOR_TABLE = 'invalidPKs';

    public function getCreateTempTableCommand(string $schemaName, string $tableName, ColumnCollection $columns): string
    {
        // TODO: Implement getCreateTempTableCommand() method.
        throw new \Exception('method is not implemented yet');
    }

    public function getDropTableCommand(string $dbName, string $tableName): string
    {
        return sprintf(
            'DROP TABLE %s.%s',
            ExasolQuote::quoteSingleIdentifier($dbName),
            ExasolQuote::quoteSingleIdentifier($tableName)
        );
    }

    public function getRenameTableCommand(string $schemaName, string $sourceTableName, string $newTableName): string
    {
        // TODO

        $quotedDbName = ExasolQuote::quoteSingleIdentifier($schemaName);
        return sprintf(
            'RENAME TABLE %s.%s TO %s.%s',
            $quotedDbName,
            ExasolQuote::quoteSingleIdentifier($sourceTableName),
            $quotedDbName,
            ExasolQuote::quoteSingleIdentifier($newTableName)
        );
    }

    public function getTruncateTableCommand(string $dbName, string $tableName): string
    {
        return sprintf(
            'TRUNCATE TABLE %s.%s',
            ExasolQuote::quoteSingleIdentifier($dbName),
            ExasolQuote::quoteSingleIdentifier($tableName)
        );
    }

    /**
     * @inheritDoc
     */
    public function getCreateTableCommand(
        string $dbName,
        string $tableName,
        ColumnCollection $columns,
        array $primaryKeys = []
    ): string {
        // TODO

        $columnNames = [];
        $columnsSqlDefinitions = [];
        /** @var ExasolColumn $column */
        foreach ($columns->getIterator() as $column) {
            $columnName = $column->getColumnName();
            $columnNames[] = $columnName;
            /** @var Exasol $columnDefinition */
            $columnDefinition = $column->getColumnDefinition();
            $columnsSqlDefinitions[] = sprintf(
                '%s %s',
                ExasolQuote::quoteSingleIdentifier($columnName),
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
                        return ExasolQuote::quoteSingleIdentifier($item);
                    }, $primaryKeys)
                )
            );
        }

        // TODO add settings to TABLE such as JOURNAL etc...
        return sprintf(
            'CREATE MULTISET TABLE %s.%s, FALLBACK
(%s);',
            ExasolQuote::quoteSingleIdentifier($dbName),
            ExasolQuote::quoteSingleIdentifier($tableName),
            $columnsSql
        );
    }
}
