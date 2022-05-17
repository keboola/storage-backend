<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Schema\Snowflake;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\DataHelper;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Schema\SchemaReflectionInterface;

final class SnowflakeSchemaReflection implements SchemaReflectionInterface
{
    private Connection $connection;

    private string $schemaName;

    public function __construct(Connection $connection, string $schemaName)
    {
        $this->schemaName = $schemaName;
        $this->connection = $connection;
    }

    /**
     * @return string[]
     */
    public function getTablesNames(): array
    {
        /** @var array<array{name:string,kind:string}> $tables */
        $tables = $this->connection->fetchAllAssociative(
            sprintf(
                'SHOW TABLES IN SCHEMA %s',
                SnowflakeQuote::quoteSingleIdentifier($this->schemaName)
            )
        );

        $tables = array_filter($tables, fn($item) => $item['kind'] === 'TABLE');

        return array_map(static fn($table) => $table['name'], $tables);
    }

    /**
     * @return string[]
     */
    public function getViewsNames(): array
    {
        /** @var array<array{name:string}> $tables */
        $tables = $this->connection->fetchAllAssociative(
            sprintf(
                'SHOW VIEWS IN SCHEMA %s',
                SnowflakeQuote::quoteSingleIdentifier($this->schemaName)
            )
        );

        return array_map(static fn($table) => $table['name'], $tables);
    }
}
