<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Auth;

use Doctrine\DBAL\DBALException;
use Keboola\TableBackendUtils\Auth\SynapseUserReflection;
use Tests\Keboola\TableBackendUtils\Functional\SynapseBaseCase;

class SynapseUserReflectionTest extends SynapseBaseCase
{
    private const LOGIN_PREFIX = 'UTILS_TEST_AUTH_LOGIN_';
    private const LOGIN_PASSWORD = 'Str0ngPassword!';

    /**
     * @var string
     *
     * User name has to be generated because when new user is created
     * deleted and created it has no access to db :/
     * there is some time to propagate these changes and test are too fast
     */
    private $currentLogin;

    protected function setUp(): void
    {
        parent::setUp();
        $masterDb = $this->getMasterDbConnection();

        // drop all users
        $users = $this->connection->fetchAll(sprintf(
            'SELECT [name] FROM [sys].[sysusers] WHERE [name] LIKE N%s',
            $this->connection->quote(self::LOGIN_PREFIX . '%')
        ));
        foreach ($users as $user) {
            $this->connection->exec(sprintf(
                'DROP USER %s',
                $this->platform->quoteSingleIdentifier($user['name'])
            ));
        }

        // drop all logins
        $logins = $masterDb->fetchAll(sprintf(
            'SELECT [name] FROM [sys].[sql_logins] WHERE [name] LIKE N%s',
            $masterDb->quote(self::LOGIN_PREFIX . '%')
        ));
        foreach ($logins as $login) {
            $masterDb->exec(sprintf(
                'DROP LOGIN %s',
                $this->platform->quoteSingleIdentifier($login['name'])
            ));
        }

        // set random user name
        $this->currentLogin = self::LOGIN_PREFIX . bin2hex(random_bytes(2));
        $loginQuoted = $this->platform->quoteSingleIdentifier($this->currentLogin);

        // create login in master
        $masterDb->exec(sprintf(
            'CREATE LOGIN %s WITH PASSWORD = %s',
            $loginQuoted,
            $masterDb->quote(self::LOGIN_PASSWORD)
        ));
        $masterDb->close();

        // create user
        $this->connection->exec(sprintf(
            'CREATE USER %s FOR LOGIN %s',
            $loginQuoted,
            $loginQuoted
        ));
    }

    public function getMasterDbConnection(): \Doctrine\DBAL\Connection
    {
        return \Doctrine\DBAL\DriverManager::getConnection([
            'user' => getenv('SYNAPSE_UID'),
            'password' => getenv('SYNAPSE_PWD'),
            'host' => getenv('SYNAPSE_SERVER'),
            'dbname' => 'master',
            'port' => 1433,
            'driver' => 'pdo_sqlsrv',
        ]);
    }

    public function testEndAllUserSessions(): void
    {
        $ref = new SynapseUserReflection($this->connection, $this->currentLogin);
        $this->assertCount(0, $ref->getAllSessionIds());

        // connect as user
        $dbUser = $this->getTestLoginConnection();
        $dbUser->connect();

        $ref = new SynapseUserReflection($this->connection, $this->currentLogin);

        $this->assertGreaterThan(0, count($ref->getAllSessionIds()));

        $dbUser->fetchAll('SELECT * FROM sys.tables');

        $ref->endAllSessions();

        $this->expectException(DBALException::class);
        $dbUser->fetchAll('SELECT * FROM sys.tables');
    }

    public function getTestLoginConnection(): \Doctrine\DBAL\Connection
    {
        return \Doctrine\DBAL\DriverManager::getConnection([
            'user' => $this->currentLogin,
            'password' => self::LOGIN_PASSWORD,
            'host' => getenv('SYNAPSE_SERVER'),
            'dbname' => getenv('SYNAPSE_DATABASE'),
            'port' => 1433,
            'driver' => 'pdo_sqlsrv',
        ]);
    }
}
