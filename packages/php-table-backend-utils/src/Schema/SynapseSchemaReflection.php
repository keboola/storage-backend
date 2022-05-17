<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Schema;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Escaping\SynapseQuote;

final class SynapseSchemaReflection implements SchemaReflectionInterface
{
    private \Doctrine\DBAL\Connection $connection;

    private string $schemaName;

    public function __construct(Connection $connection, string $schemaName)
    {
        $this->schemaName = $schemaName;
        $this->connection = $connection;
    }

    public function getTablesNames(): array
    {
        $schema = SynapseQuote::quote($this->schemaName);
        /** @var array<array{name:string}> $tables */
        $tables = $this->connection->fetchAllAssociative(
            <<< EOT
SELECT name
FROM sys.tables
WHERE schema_name(schema_id) = $schema
order by name;
EOT
        );

        return array_map(static fn($table) => $table['name'], $tables);
    }

    public function getViewsNames(): array
    {
        $schema = SynapseQuote::quote($this->schemaName);
        /** @var array<array{name:string}> $tables */
        $tables = $this->connection->fetchAllAssociative(
            <<< EOT
SELECT name
FROM sys.views
WHERE schema_name(schema_id) = $schema
order by name;
EOT
        );

        return array_map(static fn($table) => $table['name'], $tables);
    }
}
