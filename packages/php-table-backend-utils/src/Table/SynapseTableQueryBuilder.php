<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;
use Keboola\TableBackendUtils\Escaping\SynapseQuote;
use Keboola\TableBackendUtils\QueryBuilderException;
use Keboola\TableBackendUtils\Table\Synapse\TableIndexDefinition;

class SynapseTableQueryBuilder implements TableQueryBuilderInterface
{

    public function getCreateTempTableCommand(
        string $schemaName,
        string $tableName,
        ColumnCollection $columns
    ): string {
        $this->assertTemporaryTable($tableName);

        $columnsSql = [];
        foreach ($columns as $column) {
            $columnsSql[] = sprintf(
                '%s %s',
                SynapseQuote::quoteSingleIdentifier($column->getColumnName()),
                $column->getColumnDefinition()->getSQLDefinition()
            );
        }

        return sprintf(
            'CREATE TABLE %s.%s (%s) WITH (HEAP, LOCATION = USER_DB)',
            SynapseQuote::quoteSingleIdentifier($schemaName),
            SynapseQuote::quoteSingleIdentifier($tableName),
            implode(', ', $columnsSql)
        );
    }

    private function assertTemporaryTable(string $tableName): void
    {
        if (strpos($tableName, '#') !== 0 || $tableName === '#') {
            throw new QueryBuilderException(
                sprintf(
                // phpcs:ignore
                    'Temporary table name invalid, temporary table name must start with "#" a not be empty "%s" supplied.',
                    $tableName
                ),
                QueryBuilderException::STRING_CODE_INVALID_TEMP_TABLE
            );
        }
    }

    public function getCreateTableCommand(
        string $schemaName,
        string $tableName,
        ColumnCollection $columns,
        array $primaryKeys = []
    ): string {
        $columnsSql = [];
        foreach ($columns as $column) {
            if ($column->getColumnName() === ColumnInterface::TIMESTAMP_COLUMN_NAME) {
                $columnsSql[] = sprintf('[%s] DATETIME2', ColumnInterface::TIMESTAMP_COLUMN_NAME);
                continue;
            }
            $columnsSql[] = sprintf(
                '%s %s',
                SynapseQuote::quoteSingleIdentifier($column->getColumnName()),
                $column->getColumnDefinition()->getSQLDefinition()
            );
        }

        $primaryKeySql = '';
        if (!empty($primaryKeys)) {
            $quotedPrimaryKeys = array_map(function ($columnName) {
                return SynapseQuote::quoteSingleIdentifier($columnName);
            }, $primaryKeys);
            $primaryKeySql = sprintf(
                ', PRIMARY KEY NONCLUSTERED(%s) NOT ENFORCED',
                implode(',', $quotedPrimaryKeys)
            );
        }

        return sprintf(
            'CREATE TABLE %s.%s (%s%s)',
            SynapseQuote::quoteSingleIdentifier($schemaName),
            SynapseQuote::quoteSingleIdentifier($tableName),
            implode(', ', $columnsSql),
            $primaryKeySql
        );
    }

    public function getCreateTableCommandFromDefinition(
        SynapseTableDefinition $definition,
        bool $definePrimaryKeys = false
    ): string {
        $columnsSql = [];
        foreach ($definition->getColumnsDefinitions() as $column) {
            $columnsSql[] = sprintf(
                '%s %s',
                SynapseQuote::quoteSingleIdentifier($column->getColumnName()),
                $column->getColumnDefinition()->getSQLDefinition()
            );
        }

        $primaryKeySql = '';
        if ($definePrimaryKeys === true && count($definition->getPrimaryKeysNames()) !== 0) {
            $quotedPrimaryKeys = array_map(function ($columnName) {
                return SynapseQuote::quoteSingleIdentifier($columnName);
            }, $definition->getPrimaryKeysNames());
            $primaryKeySql = sprintf(
                ', PRIMARY KEY NONCLUSTERED(%s) NOT ENFORCED',
                implode(',', $quotedPrimaryKeys)
            );
        }
        if ($definition->getTableDistribution()->isHashDistribution()) {
            $quotedColumns = array_map(function ($columnName) {
                return SynapseQuote::quoteSingleIdentifier($columnName);
            }, $definition->getTableDistribution()->getDistributionColumnsNames());
            $distributionSql = sprintf(
                'DISTRIBUTION = %s(%s)',
                $definition->getTableDistribution()->getDistributionName(),
                implode(',', $quotedColumns)
            );
        } else {
            $distributionSql = sprintf(
                'DISTRIBUTION = %s',
                $definition->getTableDistribution()->getDistributionName()
            );
        }
        if ($definition->getTableIndex()->getIndexType() === TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_INDEX) {
            $quotedColumns = array_map(function ($columnName) {
                return SynapseQuote::quoteSingleIdentifier($columnName);
            }, $definition->getTableIndex()->getIndexedColumnsNames());
            $indexSql = sprintf(
                '%s(%s)',
                $definition->getTableIndex()->getIndexType(),
                implode(',', $quotedColumns)
            );
        } else {
            $indexSql = $definition->getTableIndex()->getIndexType();
        }

        return sprintf(
            'CREATE TABLE %s.%s (%s%s) WITH (%s,%s)',
            SynapseQuote::quoteSingleIdentifier($definition->getSchemaName()),
            SynapseQuote::quoteSingleIdentifier($definition->getTableName()),
            implode(', ', $columnsSql),
            $primaryKeySql,
            $distributionSql,
            $indexSql
        );
    }

    public function getDropTableCommand(
        string $schemaName,
        string $tableName
    ): string {
        return sprintf(
            'DROP TABLE %s.%s',
            SynapseQuote::quoteSingleIdentifier($schemaName),
            SynapseQuote::quoteSingleIdentifier($tableName)
        );
    }

    public function getRenameTableCommand(
        string $schemaName,
        string $sourceTableName,
        string $newTableName
    ): string {
        return sprintf(
            'RENAME OBJECT %s.%s TO %s',
            SynapseQuote::quoteSingleIdentifier($schemaName),
            SynapseQuote::quoteSingleIdentifier($sourceTableName),
            SynapseQuote::quoteSingleIdentifier($newTableName)
        );
    }

    public function getTruncateTableCommand(
        string $schemaName,
        string $tableName
    ): string {
        return sprintf(
            'TRUNCATE TABLE %s.%s',
            SynapseQuote::quoteSingleIdentifier($schemaName),
            SynapseQuote::quoteSingleIdentifier($tableName)
        );
    }
}
