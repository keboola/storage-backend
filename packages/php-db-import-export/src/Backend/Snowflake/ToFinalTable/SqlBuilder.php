<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable;

use Keboola\Datatype\Definition\BaseType;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;

class SqlBuilder
{
    public const SRC_ALIAS = 'src';

    public function getCommitTransaction(): string
    {
        return 'COMMIT';
    }

    /**
     * @param string[] $primaryKeys
     */
    public function getDedupCommand(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $deduplicationTableDefinition,
        array $primaryKeys
    ): string {
        if (empty($primaryKeys)) {
            return '';
        }
// TODO

        return '';
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
            return $alias . SnowflakeQuote::quoteSingleIdentifier($columns);
        }, $columns));
    }

    public function getDeleteOldItemsCommand(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition
    ): string {
        $stagingTable = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName())
        );

        $destinationTable = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getTableName())
        );

        // TODO
        return '';
    }


    public function getDropTableIfExistsCommand(
        string $schema,
        string $tableName
    ): string {
        return sprintf(
            'DROP TABLE IF EXISTS %s.%s CASCADE',
            SnowflakeQuote::quoteSingleIdentifier($schema),
            SnowflakeQuote::quoteSingleIdentifier($tableName)
        );
    }

    public function getInsertAllIntoTargetTableCommand(
        SnowflakeTableDefinition $sourceTableDefinition,
        SnowflakeTableDefinition $destinationTableDefinition,
        SnowflakeImportOptions $importOptions,
        string $timestamp
    ): string {
        $destinationTable = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($destinationTableDefinition->getTableName())
        );

        $insColumns = $sourceTableDefinition->getColumnsNames();
        $useTimestamp = !in_array(ToStageImporterInterface::TIMESTAMP_COLUMN_NAME, $insColumns, true)
            && $importOptions->useTimestamp();

        if ($useTimestamp) {
            $insColumns = array_merge(
                $sourceTableDefinition->getColumnsNames(),
                [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME]
            );
        }

        $columnsSetSql = [];
return '';
// TODO
    }

    public function getTruncateTableWithDeleteCommand(
        string $schema,
        string $tableName
    ): string {
        return sprintf(
            'DELETE FROM %s.%s',
            SnowflakeQuote::quoteSingleIdentifier($schema),
            SnowflakeQuote::quoteSingleIdentifier($tableName)
        );
    }

    public function getUpdateWithPkCommandSubstitute(
        SnowflakeTableDefinition $stagingTableDefinition,
        SnowflakeTableDefinition $destinationDefinition,
        SnowflakeImportOptions $importOptions,
        string $timestamp
    ): string {
        return '';
        // TODO
    }

}
