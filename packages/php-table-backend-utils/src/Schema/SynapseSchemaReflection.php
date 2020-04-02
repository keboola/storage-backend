<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Schema;

use Doctrine\DBAL\Connection;

final class SynapseSchemaReflection implements SchemaReflectionInterface
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

    /**
     * @return string[]
     */
    public function getTablesNames(): array
    {
        $schema = $this->connection->quote($this->schemaName);
        $tables = $this->connection->fetchAll(
            <<< EOT
SELECT name
FROM sys.tables
WHERE schema_name(schema_id) = $schema
order by name;
EOT
        );

        return array_map(static function ($table) {
            return $table['name'];
        }, $tables);
    }
}
