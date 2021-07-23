<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Teradata;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Keboola\TableBackendUtils\Connection\Teradata\ExasolConnection;
use Keboola\TableBackendUtils\Connection\Teradata\TeradataPlatform;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use PHPUnit\Framework\TestCase;

class TeradataBaseCase extends TestCase
{
    public const TESTS_PREFIX = 'utilsTest_';

    public const TEST_DATABASE = self::TESTS_PREFIX . 'refTableDatabase';
    public const TABLE_GENERIC = self::TESTS_PREFIX . 'refTab';
    public const VIEW_GENERIC = self::TESTS_PREFIX . 'refView';

    /** @var Connection */
    protected $connection;

    /** @var TeradataPlatform|AbstractPlatform */
    protected $platform;

    protected function setUp(): void
    {
        parent::setUp();
        $this->connection = $this->getTeradataConnection();
        $this->platform = $this->connection->getDatabasePlatform();
    }

    protected function initTable(
        string $database = self::TEST_DATABASE,
        string $table = self::TABLE_GENERIC
    ): void {
        // char because of Stats test
        $this->connection->executeQuery(
            sprintf(
                'CREATE MULTISET TABLE %s.%s ,NO FALLBACK
     (
      "id" INTEGER NOT NULL,
      "first_name" CHAR(10000),
      "last_name" CHAR(10000)
     );',
                TeradataQuote::quoteSingleIdentifier($database),
                TeradataQuote::quoteSingleIdentifier($table)
            )
        );
    }

    protected function dbExists(string $dbname): bool
    {
        try {
            $this->connection->executeQuery(sprintf('HELP DATABASE %s', $dbname));
            return true;
        } catch (\Doctrine\DBAL\Exception $e) {
            return false;
        }
    }

    protected function cleanDatabase(string $dbname): void
    {
        if (!$this->dbExists($dbname)) {
            return;
        }

        // delete all objects in the DB
        $this->connection->executeQuery(sprintf('DELETE DATABASE %s ALL', $dbname));
        // drop the empty db
        $this->connection->executeQuery(sprintf('DROP DATABASE %s', $dbname));
    }

    public function createDatabase(string $dbName): void
    {
        $this->connection->executeQuery(sprintf('
CREATE DATABASE %s AS
       PERM = 1e9;
       ', $dbName));
    }

    private function getTeradataConnection(): Connection
    {
        return ExasolConnection::getConnection([
            'host' => (string) getenv('TERADATA_HOST'),
            'user' => (string) getenv('TERADATA_USERNAME'),
            'password' => (string) getenv('TERADATA_PASSWORD'),
            'port' => (int) getenv('TERADATA_PORT'),
            'dbname' => '',
        ]);
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
        // list existing users
        $existingUsers = $this->connection->fetchAllAssociative(sprintf(
            'SELECT  UserName FROM DBC.UsersV U WHERE "U"."Username" = %s',
            TeradataQuote::quote($userName)
        ));

        // delete existing users
        foreach ($existingUsers as $existingUser) {
            $this->connection->executeQuery(sprintf(
                'DROP USER %s',
                TeradataQuote::quoteSingleIdentifier($existingUser['UserName'])
            ));
        }

        // create user
        $this->connection->executeQuery(sprintf(
            'CREATE USER %s AS PERM = 0 PASSWORD="xxxx" DEFAULT DATABASE = %s;',
            TeradataQuote::quoteSingleIdentifier($userName),
            TeradataQuote::quoteSingleIdentifier($userName . 'DB')
        ));
    }

    protected function setUpRole(string $roleName): void
    {
        // list existing roles
        $existingUsers = $this->connection->fetchAllAssociative(sprintf(
            'SELECT RoleName FROM DBC.RoleInfo WHERE RoleName = %s',
            TeradataQuote::quote($roleName)
        ));

        // delete existing roles
        foreach ($existingUsers as $existingUser) {
            $this->connection->executeQuery(sprintf(
                'DROP ROLE %s',
                TeradataQuote::quoteSingleIdentifier($existingUser['RoleName'])
            ));
        }

        // create role
        $this->connection->executeQuery(sprintf(
            'CREATE ROLE %s;',
            TeradataQuote::quoteSingleIdentifier($roleName)
        ));
    }

    protected function insertRowToTable(
        string $dbName,
        string $tableName,
        int $id,
        string $firstName,
        string $lastName
    ): void {
        $this->connection->executeQuery(sprintf(
            'INSERT INTO %s.%s VALUES (%d, %s, %s)',
            TeradataQuote::quoteSingleIdentifier($dbName),
            TeradataQuote::quoteSingleIdentifier($tableName),
            $id,
            TeradataQuote::quote($firstName),
            TeradataQuote::quote($lastName)
        ));
    }
}
