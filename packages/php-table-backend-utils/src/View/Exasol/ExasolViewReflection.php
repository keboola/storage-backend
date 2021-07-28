<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\View\Exasol;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Keboola\TableBackendUtils\Escaping\SynapseQuote;
use Keboola\TableBackendUtils\View\ViewReflectionInterface;

final class ExasolViewReflection implements ViewReflectionInterface
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

    public function getDependentViews(): array
    {
        // TODO
        return [];
    }

    /**
     * if definition is longer than 4000characters, function will throw exception and user has to create view on its own
     */
    public function getViewDefinition(): string
    {
        // TODO
        return '';
    }

    /**
     * in general there is stored procedure sp_refreshview in mssql but this is not available in synapse
     * function is using INFORMATION_SCHEMA.VIEWS[VIEW_DEFINITION] to recreate view
     * if definition is longer than 4000characters, function will throw exception and user has to create view on its own
     */
    public function refreshView(): void
    {
        // todo
    }
}
