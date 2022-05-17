<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Exasol;

use Keboola\Datatype\Definition\Exasol;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Exasol\ExasolColumn;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\QueryBuilderException;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\TableQueryBuilderInterface;

class ExasolTableQueryBuilder implements TableQueryBuilderInterface
{
    private const INVALID_PKS_FOR_TABLE = 'invalidPKs';
    private const INVALID_TABLE_NAME = 'invalidTableName';

    public function getCreateTempTableCommand(string $schemaName, string $tableName, ColumnCollection $columns): string
    {
        // TODO: Implement getCreateTempTableCommand() method.
        throw new \Exception('method is not implemented yet');
    }

    public function getDropTableCommand(string $schemaName, string $tableName): string
    {
        return sprintf(
            'DROP TABLE %s.%s',
            ExasolQuote::quoteSingleIdentifier($schemaName),
            ExasolQuote::quoteSingleIdentifier($tableName)
        );
    }

    public function getRenameTableCommand(string $schemaName, string $sourceTableName, string $newTableName): string
    {
        if (!$this->validateTableName($newTableName)) {
            throw new QueryBuilderException(
                sprintf(
                    'Invalid table name %s: Only alphanumeric characters dash and underscores are allowed.',
                    $newTableName
                ),
                self::INVALID_TABLE_NAME
            );
        }

        $quotedDbName = ExasolQuote::quoteSingleIdentifier($schemaName);
        return sprintf(
            'RENAME TABLE %s.%s TO %s.%s',
            $quotedDbName,
            ExasolQuote::quoteSingleIdentifier($sourceTableName),
            $quotedDbName,
            ExasolQuote::quoteSingleIdentifier($newTableName)
        );
    }

    public function getTruncateTableCommand(string $schemaName, string $tableName): string
    {
        return sprintf(
            'TRUNCATE TABLE %s.%s',
            ExasolQuote::quoteSingleIdentifier($schemaName),
            ExasolQuote::quoteSingleIdentifier($tableName)
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
        if (!$this->validateTableName($tableName)) {
            throw new QueryBuilderException(
                sprintf(
                    'Invalid table name %s: Only alphanumeric characters dash and underscores are allowed.',
                    $tableName
                ),
                self::INVALID_TABLE_NAME
            );
        }

        $columnNames = [];
        $columnsSqlDefinitions = [];
        /** @var ExasolColumn $column */
        foreach ($columns->getIterator() as $column) {
            $columnName = $column->getColumnName();
            $columnNames[] = $columnName;
            /** @var Exasol $columnDefinition */
            $columnDefinition = $column->getColumnDefinition();

            // check if PK can be defined on selected columns
            if ($primaryKeys && in_array($columnName, $primaryKeys, true)
                && $columnDefinition->isNullable()) {
                throw new QueryBuilderException(
                    sprintf('Trying to set PK on column %s but this column is nullable', $columnName),
                    self::INVALID_PKS_FOR_TABLE
                );
            }

            $columnsSqlDefinitions[] = sprintf(
                '%s %s',
                ExasolQuote::quoteSingleIdentifier($columnName),
                $columnDefinition->getSQLDefinition()
            );
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

        if ($primaryKeys !== []) {
            $columnsSqlDefinitions[] =
                sprintf(
                    'CONSTRAINT PRIMARY KEY (%s)',
                    implode(',', array_map(static fn($item) => ExasolQuote::quoteSingleIdentifier($item), $primaryKeys))
                );
        }

        $columnsSql = implode(",\n", $columnsSqlDefinitions);

        // brackets on single rows because in order to have much more beautiful query at the end
        return sprintf(
            'CREATE TABLE %s.%s
(
%s
);',
            ExasolQuote::quoteSingleIdentifier($schemaName),
            ExasolQuote::quoteSingleIdentifier($tableName),
            $columnsSql
        );
    }

    /**
     * @param ExasolTableDefinition $definition
     */
    public function getCreateTableCommandFromDefinition(
        TableDefinitionInterface $definition,
        bool $definePrimaryKeys = self::CREATE_TABLE_WITHOUT_PRIMARY_KEYS
    ): string {
        assert($definition instanceof ExasolTableDefinition);
        return $this->getCreateTableCommand(
            $definition->getSchemaName(),
            $definition->getTableName(),
            $definition->getColumnsDefinitions(),
            $definePrimaryKeys === self::CREATE_TABLE_WITH_PRIMARY_KEYS
                ? $definition->getPrimaryKeysNames()
                : []
        );
    }

    private function validateTableName(string $tableName): bool
    {
        return (bool) preg_match('/^[-_A-Za-z0-9]+$/', $tableName, $out);
    }
}
