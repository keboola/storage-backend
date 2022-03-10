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
        // TODO
        return '';
    }

    public function getDropTableIfExistsCommand(string $getSchemaName, string $string): string
    {
        // TODO
        return '';
    }

    public function getInsertAllIntoTargetTableCommand(
        TeradataTableDefinition $stagingTableDefinition,
        TeradataTableDefinition $destinationTableDefinition,
        TeradataImportOptions $options,
        string $getNowFormatted
    ): string {
        return '';
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
