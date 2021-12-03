<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\View\Snowflake;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\View\InvalidViewDefinitionException;
use Keboola\TableBackendUtils\View\ViewReflectionInterface;

final class SnowflakeViewReflection implements ViewReflectionInterface
{
    /** @var Connection */
    private $connection;

    /** @var string */
    private $schemaName;

    /** @var string */
    private $viewName;

    public function __construct(Connection $connection, string $schemaName, string $viewName)
    {
        $this->viewName = $viewName;
        $this->schemaName = $schemaName;
        $this->connection = $connection;
    }

    /**
     * @return array<int, array<string, mixed>>
     * array{
     *  schema_name: string,
     *  name: string
     * }[]
     */
    public function getDependentViews(): array
    {
        $sql = sprintf(
            '
SELECT 
    "OBJECT_SCHEMA" AS "schema_name", 
    "OBJECT_NAME" AS "name" 
FROM "SYS"."EXA_ALL_DEPENDENCIES"  
WHERE "REFERENCED_OBJECT_SCHEMA" = %s AND "REFERENCED_OBJECT_NAME" = %s',
            SnowflakeQuote::quote($this->schemaName),
            SnowflakeQuote::quote($this->viewName)
        );

        return $this->connection->fetchAllAssociative($sql);
    }

    public function getViewDefinition(): string
    {
        $sql = sprintf(
            'SELECT "VIEW_TEXT" FROM "SYS"."EXA_ALL_VIEWS"  WHERE "VIEW_SCHEMA" = %s AND "VIEW_NAME" = %s',
            SnowflakeQuote::quote($this->schemaName),
            SnowflakeQuote::quote($this->viewName)
        );

        return $this->connection->fetchOne($sql);
    }

    public function refreshView(): void
    {
        $definition = $this->getViewDefinition();

        $objectNameWithSchema = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($this->schemaName),
            SnowflakeQuote::quoteSingleIdentifier($this->viewName)
        );

        $this->connection->executeQuery(sprintf('DROP VIEW %s', $objectNameWithSchema));
        try {
            $this->connection->executeQuery($definition);
        } catch (Exception $e) {
            throw InvalidViewDefinitionException::createViewRefreshError($this->schemaName, $this->viewName, $e);
        }
    }
}
