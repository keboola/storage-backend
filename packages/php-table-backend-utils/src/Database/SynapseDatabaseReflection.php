<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Database;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Escaping\SynapseQuote;

final class SynapseDatabaseReflection implements DatabaseReflectionInterface
{
    private \Doctrine\DBAL\Connection $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * @return string[]
     */
    public function getUsersNames(?string $like = null): array
    {
        $where = ' WHERE [issqluser] = 1';
        if ($like !== null) {
            $where = sprintf(
                ' WHERE [name] LIKE N%s AND [issqluser] = 1',
                SynapseQuote::quote($like)
            );
        }
        /** @var array<array{name:string}> $users */
        $users = $this->connection->fetchAllAssociative(sprintf(
            'SELECT [name] FROM [sys].[sysusers]%s',
            $where
        ));

        return array_map(static fn($record) => $record['name'], $users);
    }

    /**
     * @return string[]
     */
    public function getRolesNames(?string $like = null): array
    {
        $where = ' WHERE [issqlrole] = 1';
        if ($like !== null) {
            $where = sprintf(
                ' WHERE [name] LIKE N%s AND [issqlrole] = 1',
                SynapseQuote::quote($like)
            );
        }

        /** @var array<array{name:string}> $roles */
        $roles = $this->connection->fetchAllAssociative(sprintf(
            'SELECT [name] FROM [sys].[sysusers]%s',
            $where
        ));

        return array_map(static fn($record) => $record['name'], $roles);
    }
}
