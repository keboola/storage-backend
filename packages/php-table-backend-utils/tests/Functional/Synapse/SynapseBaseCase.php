<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Synapse;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Keboola\TableBackendUtils\Connection\Synapse\SynapseDriver;
use Keboola\TableBackendUtils\Schema\SynapseSchemaQueryBuilder;
use Keboola\TableBackendUtils\Schema\SynapseSchemaReflection;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use PHPUnit\Framework\TestCase;

class SynapseBaseCase extends TestCase
{
    public const TESTS_PREFIX = 'utils-test_';

    protected Connection $connection;

    /** @var SQLServer2012Platform|AbstractPlatform */
    protected $platform;

    protected SynapseSchemaQueryBuilder $schemaQb;

    protected SynapseTableQueryBuilder $tableQb;

    protected function dropAllWithinSchema(string $schema): void
    {
        $ref = new SynapseSchemaReflection($this->connection, $schema);
        $tables = $ref->getTablesNames();

        foreach ($tables as $table) {
            $this->connection->executeStatement(
                $this->tableQb->getDropTableCommand($schema, $table)
            );
        }

        $ref = new SynapseSchemaReflection($this->connection, $schema);
        $views = $ref->getViewsNames();

        foreach ($views as $view) {
            $this->connection->executeStatement(sprintf('DROP VIEW [%s].[%s]', $schema, $view));
        }

        /** @var array{array{name:string}} $schemas */
        $schemas = $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT name FROM sys.schemas WHERE name = \'%s\'',
                $schema
            )
        );

        foreach ($schemas as $item) {
            $this->connection->executeStatement(
                $this->schemaQb->getDropSchemaCommand($item['name'])
            );
        }
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->connection = $this->getSynapseConnection();
        $this->platform = $this->connection->getDatabasePlatform();
        $this->schemaQb = new SynapseSchemaQueryBuilder();
        $this->tableQb = new SynapseTableQueryBuilder();
    }

    private function getSynapseConnection(): Connection
    {
        return DriverManager::getConnection([
            'driverClass' => SynapseDriver::class,
            'user' => (string) getenv('SYNAPSE_UID'),
            'password' => (string) getenv('SYNAPSE_PWD'),
            'host' => (string) getenv('SYNAPSE_SERVER'),
            'dbname' => (string) getenv('SYNAPSE_DATABASE'),
            'port' => 1433,
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
