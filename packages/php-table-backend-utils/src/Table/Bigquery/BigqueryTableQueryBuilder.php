<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Bigquery;

use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Datatype\Definition\Common;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\QueryBuilderException;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\TableQueryBuilderInterface;
use LogicException;

class BigqueryTableQueryBuilder implements TableQueryBuilderInterface
{
    private const INVALID_DEFAULT_VALUE_FOR_NUMERIC_COLUMN = 'invalidDefaultValueForNumericColumn';
    private const INVALID_DEFAULT_VALUE_FOR_BOOLAN_COLUMN = 'invalidDefaultValueForBooleanColumn';
    private const INVALID_KEY_TO_UPDATE = 'invalidKeyToUpdate';

    /** @var string[] */
    protected static array $boolishValues = ['true', 'false', '0', '1'];

    public function getCreateTempTableCommand(string $schemaName, string $tableName, ColumnCollection $columns): string
    {
        throw new LogicException('Not implemented');
    }

    public function getDropTableCommand(string $schemaName, string $tableName): string
    {
        return sprintf(
            'DROP TABLE %s.%s',
            BigqueryQuote::quoteSingleIdentifier($schemaName),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        );
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
        array $primaryKeys = [],
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
                $columnDefinition->getSQLDefinition(),
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
            $columnsSql,
        );
    }

    public function getCreateTableCommandFromDefinition(
        TableDefinitionInterface $definition,
        bool $definePrimaryKeys = self::CREATE_TABLE_WITHOUT_PRIMARY_KEYS,
    ): string {
        assert($definition instanceof BigqueryTableDefinition);
        return $this->getCreateTableCommand(
            $definition->getSchemaName(),
            $definition->getTableName(),
            $definition->getColumnsDefinitions(),
            $definePrimaryKeys === self::CREATE_TABLE_WITH_PRIMARY_KEYS
                ? $definition->getPrimaryKeysNames()
                : [],
        );
    }

    public function getAddColumnCommand(string $schemaName, string $tableName, BigqueryColumn $columnDefinition): string
    {
        assert(
            $columnDefinition->getColumnDefinition()->getDefault() === null,
            'You cannot add a REQUIRED column to an existing table schema.',
        );
        assert(
            $columnDefinition->getColumnDefinition()->isNullable() === true,
            'You cannot add a REQUIRED column to an existing table schema.',
        );
        return sprintf(
            'ALTER TABLE %s.%s ADD COLUMN %s %s',
            BigqueryQuote::quoteSingleIdentifier($schemaName),
            BigqueryQuote::quoteSingleIdentifier($tableName),
            BigqueryQuote::quoteSingleIdentifier($columnDefinition->getColumnName()),
            $columnDefinition->getColumnDefinition()->getSQLDefinition(),
        );
    }

    public function getDropColumnCommand(string $schemaName, string $tableName, string $columnName): string
    {
        return sprintf(
            'ALTER TABLE %s.%s DROP COLUMN %s',
            BigqueryQuote::quoteSingleIdentifier($schemaName),
            BigqueryQuote::quoteSingleIdentifier($tableName),
            BigqueryQuote::quoteSingleIdentifier($columnName),
        );
    }

    /**
     * @param string[] $metadataKeysToUpdate
     * @return string[]
     */
    public function getUpdateColumnFromDefinitionQuery(
        Bigquery $desiredColumnDefinition,
        string $schemaName,
        string $tableName,
        string $columnName,
        array $metadataKeysToUpdate = [],
    ): array {
        $commands = [];

        foreach ($metadataKeysToUpdate as $metadataKey) {
            if (!in_array($metadataKey, Common::KBC_METADATA_KEYS_FOR_COLUMNS_SYNC, true)) {
                throw new QueryBuilderException(
                    sprintf(
                        'Unknown metadata key to "%s" to update.',
                        $metadataKey,
                    ),
                    self::INVALID_KEY_TO_UPDATE,
                );
            }

            switch ($metadataKey) {
                case Common::KBC_METADATA_KEY_NULLABLE:
                    // desired value has to be checked because it does not set value but only drops nullability
                    if ($desiredColumnDefinition->isNullable()) {
                        $commands[$metadataKey] = sprintf(
                            'ALTER TABLE %s.%s ALTER COLUMN %s DROP NOT NULL;',
                            BigqueryQuote::quoteSingleIdentifier($schemaName),
                            BigqueryQuote::quoteSingleIdentifier($tableName),
                            BigqueryQuote::quoteSingleIdentifier($columnName),
                        );
                    }
                    break;
                case Common::KBC_METADATA_KEY_DEFAULT:
                    // set default
                    $commands[$metadataKey] = sprintf(
                        'ALTER TABLE %s.%s ALTER COLUMN %s SET DEFAULT %s;',
                        BigqueryQuote::quoteSingleIdentifier($schemaName),
                        BigqueryQuote::quoteSingleIdentifier($tableName),
                        BigqueryQuote::quoteSingleIdentifier($columnName),
                        $this->transformDefaultValue($columnName, $desiredColumnDefinition),
                    );
                    break;
                case Common::KBC_METADATA_KEY_LENGTH:
                    $commands[$metadataKey] = sprintf(
                        'ALTER TABLE %s.%s ALTER COLUMN %s SET DATA TYPE %s;',
                        BigqueryQuote::quoteSingleIdentifier($schemaName),
                        BigqueryQuote::quoteSingleIdentifier($tableName),
                        BigqueryQuote::quoteSingleIdentifier($columnName),
                        $desiredColumnDefinition->getTypeOnlySQLDefinition(),
                    );
                    break;
            }
        }

        return $commands;
    }

    protected function transformDefaultValue(string $columnName, Bigquery $desiredDefinition): string
    {
        switch (true) {
            case $desiredDefinition->getDefault() === null:
                return 'NULL';
            case $desiredDefinition->getBasetype() === BaseType::BOOLEAN:
                if (!in_array($desiredDefinition->getDefault(), self::$boolishValues)) {
                    throw new QueryBuilderException(
                        sprintf(
                            'Invalid default value for column "%s". Allowed values are %s, got "%s".',
                            $columnName,
                            implode(', ', self::$boolishValues),
                            $desiredDefinition->getDefault(),
                        ),
                        self::INVALID_DEFAULT_VALUE_FOR_BOOLAN_COLUMN,
                    );
                }
                return $this->castToBool($desiredDefinition->getDefault()) ? 'TRUE' : 'FALSE';
            case in_array($desiredDefinition->getBasetype(), [BaseType::INTEGER, BaseType::NUMERIC, BaseType::FLOAT]):
                if (is_numeric($desiredDefinition->getDefault())) {
                    return $desiredDefinition->getDefault();
                }
                throw new QueryBuilderException(
                    sprintf(
                        'Invalid default value for column "%s". Expected numeric value, got "%s".',
                        $columnName,
                        $desiredDefinition->getDefault(),
                    ),
                    self::INVALID_DEFAULT_VALUE_FOR_NUMERIC_COLUMN,
                );
            default:
                return BigqueryQuote::quote($desiredDefinition->getDefault());
        }
    }

    public function castToBool(?string $castToBool): bool
    {
        if ($castToBool === '1') {
            return true;
        }
        if (is_string($castToBool) && strtolower($castToBool) === 'true') {
            return true;
        }

        return false;
    }
}
