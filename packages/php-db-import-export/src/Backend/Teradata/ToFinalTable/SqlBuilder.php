<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable;

use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;

class SqlBuilder
{
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
        TeradataTableDefinition $stagingTableDefinition,
        TeradataTableDefinition $destinationTableDefinition,
        TeradataImportOptions $options,
        string $getNowFormatted
    ): string {
        //TODO
        throw new \Exception('not implemented yet');
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
}
