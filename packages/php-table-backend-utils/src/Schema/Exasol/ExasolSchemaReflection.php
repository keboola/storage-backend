<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Schema\Exasol;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\DataHelper;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Schema\SchemaReflectionInterface;

final class ExasolSchemaReflection implements SchemaReflectionInterface
{
    /** @var Connection */
    private $connection;

    /** @var string */
    private $schemaName;

    public function __construct(Connection $connection, string $schemaName)
    {
        $this->schemaName = $schemaName;
        $this->connection = $connection;
    }

    public function getTablesNames(): array
    {
        $tables = $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT "TABLE_NAME" FROM "SYS"."EXA_ALL_TABLES" WHERE "TABLE_SCHEMA" = %s',
                ExasolQuote::quote($this->schemaName)
            )
        );

        return DataHelper::extractByKey($tables, 'TABLE_NAME');
    }

    public function getViewsNames(): array
    {
        $tables = $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT "VIEW_NAME" FROM "SYS"."EXA_ALL_VIEWS" WHERE "VIEW_SCHEMA" = %s',
                ExasolQuote::quote($this->schemaName)
            )
        );

        return DataHelper::extractByKey($tables, 'VIEW_NAME');
    }
}
