<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Teradata;

use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\TableQueryBuilderInterface;

class TeradataTableQueryBuilder implements TableQueryBuilderInterface
{
    public function getCreateTempTableCommand(string $schemaName, string $tableName, ColumnCollection $columns): string
    {
        // TODO: Implement getCreateTempTableCommand() method.
        return '';
    }

    public function getDropTableCommand(string $dbName, string $tableName): string
    {
        return sprintf(
            'DROP TABLE %s.%s',
            TeradataQuote::quoteSingleIdentifier($dbName),
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

    public function getTruncateTableCommand(string $dbName, string $tableName): string
    {
        return sprintf(
            'DELETE %s.%s ALL',
            TeradataQuote::quoteSingleIdentifier($dbName),
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
        // TODO: Implement getCreateTableCommand() method.
        return '';
    }
}
