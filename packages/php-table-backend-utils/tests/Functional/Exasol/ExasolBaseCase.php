<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Exasol;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Connection\Exasol\ExasolConnectionFactory;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use PHPUnit\Framework\TestCase;

class ExasolBaseCase extends TestCase
{
    public const TESTS_PREFIX = 'utilsTest_';
    public const TEST_SCHEMA = self::TESTS_PREFIX . 'refTableSchema';
    public const TABLE_GENERIC = self::TESTS_PREFIX . 'refTab';
    public const VIEW_GENERIC = self::TESTS_PREFIX . 'refView';

    /** @var Connection */
    protected $connection;

    protected function setUp(): void
    {
        parent::setUp();
        $this->connection = $this->getExasolConnection();
    }

    private function getExasolConnection(): Connection
    {
        return ExasolConnectionFactory::getConnection(
            (string) getenv('EXASOL_HOST'),
            (string) getenv('EXASOL_USERNAME'),
            (string) getenv('EXASOL_PASSWORD')
        );
    }

    protected function initTable(
        string $database = self::TEST_SCHEMA,
        string $table = self::TABLE_GENERIC
    ): void {
        $this->createSchema($database);
        // char because of Stats test
        $this->connection->executeQuery(
            sprintf(
                'CREATE OR REPLACE TABLE %s.%s (
            "id" INTEGER,
    "first_name" VARCHAR(100),
    "last_name" VARCHAR(100)
);',
                ExasolQuote::quoteSingleIdentifier($database),
                ExasolQuote::quoteSingleIdentifier($table)
            )
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
                ExasolQuote::quoteSingleIdentifier($schemaName)
            )
        );
    }

    protected function schemaExists(string $schemaName): bool
    {
        return (bool) $this->connection->fetchOne(
            sprintf(
                'SELECT "SCHEMA_NAME" FROM "SYS"."EXA_ALL_SCHEMAS" WHERE "SCHEMA_NAME" = %s',
                ExasolQuote::quote($schemaName)
            )
        );
    }

    public function createSchema(string $schemaName): void
    {
        // char because of Stats test
        $this->connection->executeQuery(
            sprintf(
                'CREATE SCHEMA %s;',
                ExasolQuote::quoteSingleIdentifier($schemaName)
            )
        );
    }

    public function testConnection(): void
    {
        self::assertEquals(
            1,
            $this->connection->fetchOne('SELECT 1')
        );
    }

    protected function tearDown(): void
    {
        $this->connection->close();
        parent::tearDown();
    }

    protected function setUpUser(string $userName): void
    {
        // delete existing users
        $this->connection->executeQuery(sprintf(
            'DROP USER IF EXISTS %s CASCADE',
            ExasolQuote::quoteSingleIdentifier($userName)
        ));

        // create user
        $this->connection->executeQuery(sprintf(
            'CREATE USER %s IDENTIFIED BY "xxxx";',
            ExasolQuote::quoteSingleIdentifier($userName)
        ));
    }

    protected function setUpRole(string $roleName): void
    {
        // delete existing role
        $this->connection->executeQuery(sprintf(
            'DROP ROLE IF EXISTS %s CASCADE',
            ExasolQuote::quoteSingleIdentifier($roleName)
        ));

        // create role
        $this->connection->executeQuery(sprintf(
            'CREATE ROLE %s;',
            ExasolQuote::quoteSingleIdentifier($roleName)
        ));
    }

    protected function insertRowToTable(
        string $schemaName,
        string $tableName,
        int $id,
        string $firstName,
        string $lastName
    ): void {
        $this->connection->executeQuery(sprintf(
            'INSERT INTO %s.%s VALUES (%d, %s, %s)',
            ExasolQuote::quoteSingleIdentifier($schemaName),
            ExasolQuote::quoteSingleIdentifier($tableName),
            $id,
            ExasolQuote::quote($firstName),
            ExasolQuote::quote($lastName)
        ));
    }
}
