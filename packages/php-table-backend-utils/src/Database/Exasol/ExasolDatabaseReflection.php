<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Database\Exasol;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Database\DatabaseReflectionInterface;
use Keboola\TableBackendUtils\DataHelper;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;

final class ExasolDatabaseReflection implements DatabaseReflectionInterface
{
    private Connection $connection;

    /** @var string[] */
    private static array $excludedUsers = [
        'SYS',
    ];

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * @return string[]
     */
    public function getUsersNames(?string $like = null): array
    {
        // build escaped list of system users
        $where = sprintf(
            '"U"."USER_NAME" NOT IN (%s)',
            implode(', ', array_map(static fn($item) => ExasolQuote::quote($item), self::$excludedUsers)),
        );

        // add LIKE
        if ($like !== null) {
            $where .= sprintf(
                ' AND "U"."USER_NAME" LIKE %s',
                ExasolQuote::quote("%$like%"),
            );
        }

        // load the data
        /** @var array<array{USER_NAME:string}> $users */
        $users = $this->connection->fetchAllAssociative(sprintf(
            'SELECT "U"."USER_NAME" FROM "SYS"."EXA_ALL_USERS" "U" WHERE %s',
            $where,
        ));

        // extract data to primitive array
        return array_map(static fn($record) => $record['USER_NAME'], $users);
    }

    /**
     * @return string[]
     */
    public function getRolesNames(?string $like = null): array
    {
        // build WHERE clausule
        $where = '';
        if ($like !== null) {
            $where = sprintf(
                ' WHERE "ROLE_NAME" LIKE %s',
                ExasolQuote::quote("%$like%"),
            );
        }

        // load data
        /** @var array<array{ROLE_NAME:string}> $roles */
        $roles = $this->connection->fetchAllAssociative(sprintf(
            'SELECT "ROLE_NAME" FROM "SYS"."EXA_ALL_ROLES" %s',
            $where,
        ));

        return array_map(static fn($record) => $record['ROLE_NAME'], $roles);
    }
}
