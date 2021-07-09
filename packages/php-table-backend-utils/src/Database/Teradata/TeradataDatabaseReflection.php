<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Database\Teradata;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Database\DatabaseReflectionInterface;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

final class TeradataDatabaseReflection implements DatabaseReflectionInterface
{
    /** @var Connection */
    private $connection;

    /** @var string[] */
    private static $excludedUsers = [
        'TDPUSER',
        'Crashdumps',
        'tdwm',
        'DBC',
        'LockLogShredder',
        'TDMaps',
        'Sys_Calendar',
        'SysAdmin',
        'SystemFe',
        'External_AP',
        'console',
        'viewpoint',
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
        $where = sprintf('U.UserName NOT IN (%s)', implode(', ', array_map(
            static function ($item) {
                return TeradataQuote::quote($item);
            },
            self::$excludedUsers
        )));

        // add LIKE
        if ($like !== null) {
            $where .= sprintf(
                ' AND U.UserName LIKE %s',
                TeradataQuote::quote("%$like%")
            );
        }

        // load the data
        $users = $this->connection->fetchAllAssociative(sprintf(
            'SELECT U.UserName FROM DBC.UsersV U WHERE %s',
            $where
        ));

        // extract data to primitive array
        return array_map(static function ($record) {
            return $record['UserName'];
        }, $users);
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
                ' WHERE RoleName LIKE %s',
                TeradataQuote::quote("%$like%")
            );
        }

        // load data
        $roles = $this->connection->fetchAllAssociative(sprintf(
            'SELECT RoleName FROM RoleInfo %s',
            $where
        ));

        // extract data to primitive array. Has to be trimmed because it comes with some extra spaces
        return array_map(static function ($record) {
            return trim($record['RoleName']);
        }, $roles);
    }
}
