<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\View;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\ReflectionException;

final class SynapseViewReflection implements ViewReflectionInterface
{
    /** @var Connection */
    private $connection;

    /** @var string */
    private $schemaName;

    /** @var string */
    private $viewName;

    /** @var SQLServer2012Platform|AbstractPlatform */
    private $platform;

    public function __construct(Connection $connection, string $schemaName, string $viewName)
    {
        $this->viewName = $viewName;
        $this->schemaName = $schemaName;
        $this->connection = $connection;
        $this->platform = $connection->getDatabasePlatform();
    }

    public function getDependentViews(): array
    {
        $sql = <<<EOT
            SELECT schema_name(o.schema_id) schema_name, o.name
            FROM sys.sql_expression_dependencies rel
            JOIN sys.views o ON rel.referencing_id = o.object_id
            WHERE
                rel.referenced_id = object_id(N%s)
EOT;
        /**
         * @var array{
         *  schema_name: string,
         *  name: string
         * }[] $views
         */
        $views = $this->connection->fetchAll(sprintf(
            $sql,
            $this->connection->quote(sprintf(
                '%s.%s',
                $this->platform->quoteSingleIdentifier($this->schemaName),
                $this->platform->quoteSingleIdentifier($this->viewName)
            ))
        ));

        return $views;
    }
}
