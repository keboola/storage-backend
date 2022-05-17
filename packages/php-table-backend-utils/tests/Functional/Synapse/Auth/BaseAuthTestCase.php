<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Synapse\Auth;

use Keboola\TableBackendUtils\Escaping\SynapseQuote;
use Tests\Keboola\TableBackendUtils\Functional\Synapse\SynapseBaseCase;

class BaseAuthTestCase extends SynapseBaseCase
{
    private const LOGIN_PASSWORD = 'Str0ngPassword!';

    /**
     * User name has to be generated because when new user is created
     * deleted and created it has no access to db :/
     * there is some time to propagate these changes and test are too fast
     */
    protected ?string $currentLogin = null;

    protected function getTestLoginConnection(): \Doctrine\DBAL\Connection
    {
        assert($this->currentLogin !== null);
        return \Doctrine\DBAL\DriverManager::getConnection([
            'user' => $this->currentLogin,
            'password' => self::LOGIN_PASSWORD,
            'host' => (string) getenv('SYNAPSE_SERVER'),
            'dbname' => (string) getenv('SYNAPSE_DATABASE'),
            'port' => 1433,
            'driver' => 'pdo_sqlsrv',
        ]);
    }

    protected function getMasterDbConnection(): \Doctrine\DBAL\Connection
    {
        return \Doctrine\DBAL\DriverManager::getConnection([
            'user' => (string) getenv('SYNAPSE_UID'),
            'password' => (string) getenv('SYNAPSE_PWD'),
            'host' => (string) getenv('SYNAPSE_SERVER'),
            'dbname' => 'master',
            'port' => 1433,
            'driver' => 'pdo_sqlsrv',
        ]);
    }

    protected function dropRoles(string $prefix): void
    {
        // drop all roles
        /** @var array<array{name:string}> $roles */
        $roles = $this->connection->fetchAllAssociative(sprintf(
            'SELECT [name] FROM [sys].[sysusers] WHERE [name] LIKE N%s AND [issqlrole] = 1',
            SynapseQuote::quote($prefix . '%')
        ));
        foreach ($roles as $role) {
            $this->connection->executeStatement(sprintf(
                'DROP ROLE %s',
                SynapseQuote::quoteSingleIdentifier($role['name'])
            ));
        }
    }

    protected function setUpUser(string $loginPrefix): void
    {
        $masterDb = $this->getMasterDbConnection();

        // drop all users
        /** @var array<array{name:string}> $users */
        $users = $this->connection->fetchAllAssociative(sprintf(
            'SELECT [name] FROM [sys].[sysusers] WHERE [name] LIKE N%s AND [issqluser] = 1',
            SynapseQuote::quote($loginPrefix . '%')
        ));
        foreach ($users as $user) {
            $this->connection->executeStatement(sprintf(
                'DROP USER %s',
                SynapseQuote::quoteSingleIdentifier($user['name'])
            ));
        }

        // drop all logins
        /** @var array<array{name:string}> $logins */
        $logins = $masterDb->fetchAllAssociative(sprintf(
            'SELECT [name] FROM [sys].[sql_logins] WHERE [name] LIKE N%s',
            SynapseQuote::quote($loginPrefix . '%')
        ));
        foreach ($logins as $login) {
            $masterDb->executeStatement(sprintf(
                'DROP LOGIN %s',
                SynapseQuote::quoteSingleIdentifier($login['name'])
            ));
        }

        // set random user name
        $this->currentLogin = $loginPrefix . bin2hex(random_bytes(2));
        $loginQuoted = SynapseQuote::quoteSingleIdentifier($this->currentLogin);

        // create login in master
        $masterDb->executeStatement(sprintf(
            'CREATE LOGIN %s WITH PASSWORD = %s',
            $loginQuoted,
            SynapseQuote::quote(self::LOGIN_PASSWORD)
        ));
        $masterDb->close();

        // create user
        $this->connection->executeStatement(sprintf(
            'CREATE USER %s FOR LOGIN %s',
            $loginQuoted,
            $loginQuoted
        ));
    }
}
