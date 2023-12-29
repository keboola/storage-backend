<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Snowflake;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeConnectionFactory;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use PHPUnit\Framework\TestCase;

class SnowflakeBaseCase extends TestCase
{
    public const TESTS_PREFIX = 'utilsTest_';
    public const TEST_SCHEMA = self::TESTS_PREFIX . 'refTableSchema';
    public const TABLE_GENERIC = self::TESTS_PREFIX . 'refTab';
    public const VIEW_GENERIC = self::TESTS_PREFIX . 'refView';

    protected Connection $connection;

    protected function setUp(): void
    {
        parent::setUp();
        $this->connection = $this->getConnection();
    }

    private function getConnection(): Connection
    {
        return SnowflakeConnectionFactory::getConnection(
            (string) getenv('SNOWFLAKE_HOST'),
            (string) getenv('SNOWFLAKE_USER'),
            (string) getenv('SNOWFLAKE_PASSWORD'),
            [
                'port' => (string) getenv('SNOWFLAKE_PORT'),
                'warehouse' => (string) getenv('SNOWFLAKE_WAREHOUSE'),
                'database' => (string) getenv('SNOWFLAKE_DATABASE'),
            ],
        );
    }

    protected function initTable(
        string $schema = self::TEST_SCHEMA,
        string $table = self::TABLE_GENERIC,
        bool $createNewSchema = true,
    ): void {
        if ($createNewSchema) {
            $this->createSchema($schema);
        }

        // char because of Stats test
        $this->connection->executeQuery(
            sprintf(
                'CREATE OR REPLACE TABLE %s.%s (
            "id" INTEGER,
    "first_name" VARCHAR(100),
    "last_name" VARCHAR(100)
);',
                SnowflakeQuote::quoteSingleIdentifier($schema),
                SnowflakeQuote::quoteSingleIdentifier($table),
            ),
        );
    }

    public function createSchema(string $schemaName): void
    {
        $this->connection->executeQuery(
            sprintf(
                'CREATE SCHEMA %s;',
                SnowflakeQuote::quoteSingleIdentifier($schemaName),
            ),
        );
    }

    protected function cleanSchema(string $schemaName): void
    {
        if (!$this->schemaExists($schemaName)) {
            return;
        }

        $this->connection->executeQuery(
            sprintf(
                'DROP SCHEMA %s CASCADE',
                SnowflakeQuote::quoteSingleIdentifier($schemaName),
            ),
        );
    }

    protected function schemaExists(string $schemaName): bool
    {
        return (bool) $this->connection->fetchOne(
            sprintf(
                'SHOW SCHEMAS LIKE %s',
                SnowflakeQuote::quote($schemaName),
            ),
        );
    }

    public function testConnection(): void
    {
        $this->assertConnectionIsWorking($this->connection);
    }

    public function assertConnectionIsWorking(Connection $connection): void
    {
        self::assertEquals(
            1,
            $connection->fetchOne('SELECT 1'),
        );
    }

    protected function tearDown(): void
    {
        $this->connection->close();
        parent::tearDown();
    }

    protected function setUpUser(string $userName): void
    {
        // delete existing user
        $this->connection->executeQuery(sprintf(
            'DROP USER IF EXISTS %s',
            SnowflakeQuote::quoteSingleIdentifier($userName),
        ));

        // create user
        $this->connection->executeQuery(sprintf(
            'CREATE USER %s PASSWORD = %s',
            SnowflakeQuote::quoteSingleIdentifier($userName),
            SnowflakeQuote::quote(bin2hex(random_bytes(8))),
        ));
    }

    protected function setUpRole(string $roleName): void
    {
        // delete existing role
        $this->connection->executeQuery(sprintf(
            'DROP ROLE IF EXISTS %s',
            SnowflakeQuote::quoteSingleIdentifier($roleName),
        ));

        // create role
        $this->connection->executeQuery(sprintf(
            'CREATE ROLE %s;',
            SnowflakeQuote::quoteSingleIdentifier($roleName),
        ));
    }

    protected function insertRowToTable(
        string $schemaName,
        string $tableName,
        int $id,
        string $firstName,
        string $lastName,
    ): void {
        $this->connection->executeQuery(sprintf(
            'INSERT INTO %s.%s VALUES (%d, %s, %s)',
            SnowflakeQuote::quoteSingleIdentifier($schemaName),
            SnowflakeQuote::quoteSingleIdentifier($tableName),
            $id,
            SnowflakeQuote::quote($firstName),
            SnowflakeQuote::quote($lastName),
        ));
    }
}
