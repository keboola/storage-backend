<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\View;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;

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
        $sql = 'SELECT * FROM INFORMATION_SCHEMA.VIEWS';
        $views = $this->connection->fetchAll($sql);

        $objectNameWithSchema = sprintf(
            '%s.%s',
            $this->platform->quoteSingleIdentifier($this->schemaName),
            $this->platform->quoteSingleIdentifier($this->viewName)
        );

        /**
         * @var array{
         *  schema_name: string,
         *  name: string
         * }[] $dependencies
         */
        $dependencies = [];
        foreach ($views as $view) {
            // remove create view statement
            $text = str_replace('CREATE VIEW ' . $objectNameWithSchema, '', $view['VIEW_DEFINITION']);
            if (strpos($text, $objectNameWithSchema) === false) {
                continue;
            }

            $dependencies[] = [
                'schema_name' => $view['TABLE_SCHEMA'],
                'name' => $view['TABLE_NAME'],
            ];
        }

        return $dependencies;
    }

    /**
     * if definition is longer than 4000characters, function will throw exception and user has to create view on its own
     */
    public function getViewDefinition(): string
    {
        $sql = sprintf(
            'SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s',
            $this->connection->quote($this->schemaName),
            $this->connection->quote($this->viewName)
        );

        $definition = $this->connection->fetchColumn($sql);
        $isValid = preg_match('/CREATE[\s\S]*VIEW[\s\S]*AS[\s\S]*SELECT[\s\S]*FROM[\s\S]*/', $definition);
        if ($isValid === 0) {
            throw InvalidViewDefinitionException::createForMissingDefinition($this->schemaName, $this->viewName);
        }

        return $definition;
    }

    /**
     * in general there is stored procedure sp_refreshview in mssql but this is not available in synapse
     * function is using INFORMATION_SCHEMA.VIEWS[VIEW_DEFINITION] to recreate view
     * if definition is longer than 4000characters, function will throw exception and user has to create view on its own
     */
    public function refreshView(): void
    {
        $definition = $this->getViewDefinition();

        $objectNameWithSchema = sprintf(
            '%s.%s',
            $this->platform->quoteSingleIdentifier($this->schemaName),
            $this->platform->quoteSingleIdentifier($this->viewName)
        );

        $this->connection->exec(sprintf('DROP VIEW %s', $objectNameWithSchema));
        try {
            $this->connection->exec($definition);
        } catch (DBALException $e) {
            throw InvalidViewDefinitionException::createViewRefreshError($this->schemaName, $this->viewName, $e);
        }
    }
}
