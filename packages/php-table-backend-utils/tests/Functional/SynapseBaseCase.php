<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Keboola\TableBackendUtils\Schema\SynapseSchemaReflection;
use Keboola\TableBackendUtils\Schema\SynapseSchemaQueryBuilder;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use PHPUnit\Framework\TestCase;

class SynapseBaseCase extends TestCase
{
    public const TESTS_PREFIX = 'utils-test_';

    /** @var Connection */
    protected $connection;

    /** @var SQLServer2012Platform|AbstractPlatform */
    protected $platform;

    /** @var SynapseSchemaQueryBuilder */
    protected $schemaQb;

    /** @var SynapseTableQueryBuilder */
    protected $tableQb;

    protected function dropAllWithinSchema(string $schema): void
    {
        $ref = new SynapseSchemaReflection($this->connection, $schema);
        $tables = $ref->getTablesNames();

        foreach ($tables as $table) {
            $this->connection->exec(
                $this->tableQb->getDropTableCommand($schema, $table)
            );
        }

        $ref = new SynapseSchemaReflection($this->connection, $schema);
        $views = $ref->getViewsNames();

        foreach ($views as $view) {
            $this->connection->exec(sprintf('DROP VIEW [%s].[%s]', $schema, $view));
        }

        $schemas = $this->connection->fetchAll(
            sprintf(
                'SELECT name FROM sys.schemas WHERE name = \'%s\'',
                $schema
            )
        );

        foreach ($schemas as $item) {
            $this->connection->exec($this->schemaQb->getDropSchemaCommand($item['name']));
        }
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->connection = $this->getSynapseConnection();
        $this->platform = $this->connection->getDatabasePlatform();
        $this->schemaQb = new SynapseSchemaQueryBuilder($this->connection);
        $this->tableQb = new SynapseTableQueryBuilder($this->connection);
    }

    private function getSynapseConnection(): Connection
    {
        return \Doctrine\DBAL\DriverManager::getConnection([
            'user' => getenv('SYNAPSE_UID'),
            'password' => getenv('SYNAPSE_PWD'),
            'host' => getenv('SYNAPSE_SERVER'),
            'dbname' => getenv('SYNAPSE_DATABASE'),
            'port' => 1433,
            'driver' => 'pdo_sqlsrv',
            'driverOptions' => [
                'ConnectRetryCount' => 5,
                'ConnectRetryInterval' => 10,
//                \PDO::SQLSRV_ATTR_QUERY_TIMEOUT => 1
            ],
        ]);
    }

    protected function tearDown(): void
    {
        $this->connection->close();
        parent::tearDown();
    }
}
