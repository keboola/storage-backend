<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\View\Exasol;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\View\InvalidViewDefinitionException;
use Keboola\TableBackendUtils\View\ViewReflectionInterface;

final class ExasolViewReflection implements ViewReflectionInterface
{
    private Connection $connection;

    private string $schemaName;

    private string $viewName;

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
            ExasolQuote::quote($this->schemaName),
            ExasolQuote::quote($this->viewName),
        );

        return $this->connection->fetchAllAssociative($sql);
    }

    public function getViewDefinition(): string
    {
        $sql = sprintf(
            'SELECT "VIEW_TEXT" FROM "SYS"."EXA_ALL_VIEWS"  WHERE "VIEW_SCHEMA" = %s AND "VIEW_NAME" = %s',
            ExasolQuote::quote($this->schemaName),
            ExasolQuote::quote($this->viewName),
        );

        /** @var false|string $definition */
        $definition = $this->connection->fetchOne($sql);
        if ($definition === false) {
            throw InvalidViewDefinitionException::createForNotExistingView(
                $this->schemaName,
                $this->viewName,
            );
        }
        return $definition;
    }

    public function refreshView(): void
    {
        $definition = $this->getViewDefinition();

        $objectNameWithSchema = sprintf(
            '%s.%s',
            ExasolQuote::quoteSingleIdentifier($this->schemaName),
            ExasolQuote::quoteSingleIdentifier($this->viewName),
        );

        $this->connection->executeQuery(sprintf('DROP VIEW %s', $objectNameWithSchema));
        try {
            $this->connection->executeQuery($definition);
        } catch (Exception $e) {
            throw InvalidViewDefinitionException::createViewRefreshError($this->schemaName, $this->viewName, $e);
        }
    }
}
