<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Synapse\Auth;

use Tests\Keboola\TableBackendUtils\Functional\Synapse\SynapseBaseCase;

class BaseAuthTestCase extends SynapseBaseCase
{
    private const LOGIN_PASSWORD = 'Str0ngPassword!';

    /**
     * @var string
     *
     * User name has to be generated because when new user is created
     * deleted and created it has no access to db :/
     * there is some time to propagate these changes and test are too fast
     */
    protected $currentLogin;

    protected function getTestLoginConnection(): \Doctrine\DBAL\Connection
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

    protected function getMasterDbConnection(): \Doctrine\DBAL\Connection
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

    protected function dropRoles(string $prefix): void
    {
        // drop all roles
        $roles = $this->connection->fetchAllAssociative(sprintf(
            'SELECT [name] FROM [sys].[sysusers] WHERE [name] LIKE N%s AND [issqlrole] = 1',
            $this->connection->quote($prefix . '%')
        ));
        foreach ($roles as $role) {
            $this->connection->exec(sprintf(
                'DROP ROLE %s',
                $this->platform->quoteSingleIdentifier($role['name'])
            ));
        }
    }

    protected function setUpUser(string $loginPrefix): void
    {
        $masterDb = $this->getMasterDbConnection();

        // drop all users
        $users = $this->connection->fetchAllAssociative(sprintf(
            'SELECT [name] FROM [sys].[sysusers] WHERE [name] LIKE N%s AND [issqluser] = 1',
            $this->connection->quote($loginPrefix . '%')
        ));
        foreach ($users as $user) {
            $this->connection->exec(sprintf(
                'DROP USER %s',
                $this->platform->quoteSingleIdentifier($user['name'])
            ));
        }

        // drop all logins
        $logins = $masterDb->fetchAllAssociative(sprintf(
            'SELECT [name] FROM [sys].[sql_logins] WHERE [name] LIKE N%s',
            $masterDb->quote($loginPrefix . '%')
        ));
        foreach ($logins as $login) {
            $masterDb->exec(sprintf(
                'DROP LOGIN %s',
                $this->platform->quoteSingleIdentifier($login['name'])
            ));
        }

        // set random user name
        $this->currentLogin = $loginPrefix . bin2hex(random_bytes(2));
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
}
