<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Schema\Teradata;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\DataHelper;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Schema\SchemaReflectionInterface;

final class TeradataSchemaReflection implements SchemaReflectionInterface
{
    private Connection $connection;

    private string $databaseName;

    public function __construct(Connection $connection, string $databaseName)
    {
        $this->databaseName = $databaseName;
        $this->connection = $connection;
    }

    /**
     * @return string[]
     */
    public function getTablesNames(): array
    {
        $database = TeradataQuote::quote($this->databaseName);
        /** @var array<array{TableName:string}> $tables */
        $tables = $this->connection->fetchAllAssociative(
            <<< EOT
SELECT "TableName" 
FROM "DBC"."TablesVX" 
WHERE "TableKind" = 'T' AND "DataBaseName"=$database
EOT
        );

        return array_map(static fn($table) => trim($table['TableName']), $tables);
    }

    /**
     * @return string[]
     */
    public function getViewsNames(): array
    {
        $database = TeradataQuote::quote($this->databaseName);
        /** @var array<array{TableName:string}> $tables */
        $tables = $this->connection->fetchAllAssociative(
            <<< EOT
SELECT "TableName" 
FROM "DBC"."TablesVX" 
WHERE "TableKind" = 'V' AND "DataBaseName"=$database
EOT
        );

        return array_map(static fn(array $table) => trim($table['TableName']), $tables);
    }
}
