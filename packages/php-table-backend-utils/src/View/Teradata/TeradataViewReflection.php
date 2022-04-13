<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\View\Teradata;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\View\ViewReflectionInterface;

class TeradataViewReflection implements ViewReflectionInterface
{
    private Connection $connection;

    private string $databaseName;

    private string $viewName;

    public function __construct(Connection $connection, string $databaseName, string $viewName)
    {
        $this->databaseName = $databaseName;
        $this->viewName = $viewName;
        $this->connection = $connection;
    }

    public function getDependentViews(): array
    {
        $views = $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT * FROM dbc.tables WHERE tablekind=%s AND databasename=%s',
                TeradataQuote::quote('V'),
                TeradataQuote::quote($this->databaseName)
            )
        );

        /**
         * @var array{
         *  schema_name: string,
         *  name: string
         * }[] $dependencies
         */
        $dependencies = [];
        foreach ($views as $view) {
            // trim table name from teradata, returned with whitespaces
            $viewDefinition = $this->connection->fetchAllAssociative(
                sprintf(
                    'SHOW VIEW %s.%s',
                    TeradataQuote::quoteSingleIdentifier($this->databaseName),
                    TeradataQuote::quoteSingleIdentifier(trim($view['TableName']))
                )
            );

            var_export($this->connection->fetchAllAssociative(
                sprintf(
                    'SHOW VIEW %s.%s',
                    TeradataQuote::quoteSingleIdentifier($this->databaseName),
                    TeradataQuote::quoteSingleIdentifier(trim($view['TableName']))
                )
            ));

            // trim table name from teradata, returned with whitespaces
            $viewNameWithDatabase = sprintf(
                '%s.%s',
                TeradataQuote::quoteSingleIdentifier($this->databaseName),
                TeradataQuote::quoteSingleIdentifier(trim($view['TableName']))
            );

            // remove create view statement
            $text = str_replace(
                'CREATE VIEW ' . $viewNameWithDatabase . ' AS SELECT * FROM',
                '',
                $viewDefinition[0]['Request Text']
            );

            $sourceNameWithDatabase = sprintf(
                '%s.%s',
                TeradataQuote::quoteSingleIdentifier($this->databaseName),
                TeradataQuote::quoteSingleIdentifier($this->viewName)
            );

            if (strpos($text, $sourceNameWithDatabase) === false) {
                continue;
            }

            // trim table name from teradata, returned with whitespaces
            //return db in schema because teradata doesn't have schema
            $dependencies[] = [
                'schema_name' => trim($view['DatabaseName']),
                'name' => trim($view['TableName']),
            ];
        }

        return $dependencies;
    }
}
