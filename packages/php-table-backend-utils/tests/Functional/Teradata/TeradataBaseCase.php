<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Teradata;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Keboola\TableBackendUtils\Connection\Teradata\TeradataConnection;
use Keboola\TableBackendUtils\Connection\Teradata\TeradataPlatform;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use PHPUnit\Framework\TestCase;

class TeradataBaseCase extends TestCase
{
    public const TESTS_PREFIX = 'utils-test_';

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

    private function getTeradataConnection(): Connection
    {
        return TeradataConnection::getConnection([
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
}
