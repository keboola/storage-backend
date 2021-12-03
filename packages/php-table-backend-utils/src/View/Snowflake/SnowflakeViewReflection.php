<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\View\Snowflake;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
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
        return SnowflakeTableReflection::getDependentViewsForObject(
            $this->connection,
            $this->viewName,
            $this->schemaName,
            SnowflakeTableReflection::DEPENDENT_OBJECT_VIEW
        );
    }

    public function getViewDefinition(): string
    {
        $result = $this->connection->fetchAssociative(sprintf(
            'SHOW VIEWS LIKE %s IN %s',
            SnowflakeQuote::quote($this->viewName),
            SnowflakeQuote::quoteSingleIdentifier($this->schemaName)
        ));

        return $result ? $result['text'] : '';
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
