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
}
