<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Exasol;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Connection\Exasol\ExasolConnection;
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
        return ExasolConnection::getConnection(
            (string) getenv('EXASOL_HOST'),
            (string) getenv('EXASOL_USERNAME'),
            (string) getenv('EXASOL_PASSWORD')
        );
    }

    protected function initTable(
        string $database = self::TEST_SCHEMA,
        string $table = self::TABLE_GENERIC
    ): void {
        // char because of Stats test
        $this->connection->executeQuery(
            sprintf(
                'CREATE SCHEMA %s;',
                ExasolQuote::quoteSingleIdentifier($database)
            )
        );
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

    protected function cleanDatabase(string $schemaName): void
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

    public function createDatabase(string $schemaName): void
    {
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
    }

    protected function setUpRole(string $roleName): void
    {
    }

    protected function insertRowToTable(
        string $schemaName,
        string $tableName,
        int $id,
        string $firstName,
        string $lastName
    ): void {
    }
}
