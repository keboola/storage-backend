<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Bigquery;

use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\TableQueryBuilderInterface;
use LogicException;

class BigqueryTableQueryBuilder implements TableQueryBuilderInterface
{
    public function getCreateTempTableCommand(string $schemaName, string $tableName, ColumnCollection $columns): string
    {
        throw new LogicException('Not implemented');
    }

    public function getDropTableCommand(string $schemaName, string $tableName): string
    {
        throw new LogicException('Not implemented');
    }

    public function getRenameTableCommand(string $schemaName, string $sourceTableName, string $newTableName): string
    {
        throw new LogicException('Not implemented');
    }

    public function getTruncateTableCommand(string $schemaName, string $tableName): string
    {
        throw new LogicException('Not implemented');
    }

    /** @param array<string> $primaryKeys */
    public function getCreateTableCommand(
        string $schemaName,
        string $tableName,
        ColumnCollection $columns,
        array $primaryKeys = []
    ): string {
        assert(count($primaryKeys) === 0, 'primary keys aren\'t supported in BQ');
        $columnsSqlDefinitions = [];
        /** @var BigqueryColumn $column */
        foreach ($columns->getIterator() as $column) {
            $columnName = $column->getColumnName();
            $columnDefinition = $column->getColumnDefinition();

            $columnsSqlDefinitions[] = sprintf(
                '%s %s',
                BigqueryQuote::quoteSingleIdentifier($columnName),
                $columnDefinition->getSQLDefinition()
            );
        }
        $columnsSql = implode(",\n", $columnsSqlDefinitions);
        return sprintf(
            'CREATE TABLE %s.%s 
(
%s
);',
            BigqueryQuote::quoteSingleIdentifier($schemaName),
            BigqueryQuote::quoteSingleIdentifier($tableName),
            $columnsSql
        );
    }

    public function getCreateTableCommandFromDefinition(
        TableDefinitionInterface $definition,
        bool $definePrimaryKeys = self::CREATE_TABLE_WITHOUT_PRIMARY_KEYS
    ): string {
        throw new LogicException('Not implemented');
    }
}
