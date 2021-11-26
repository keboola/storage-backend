<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Snowflake;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;

class SnowflakeConnectionFactory
{
    /**
     * @param array{
     *     'port'?:string,
     *     'warehouse'?:string,
     *     'database'?:string,
     *     'schema'?:string,
     *     'tracing'?:int,
     *     'loginTimeout'?:int,
     *     'networkTimeout'?:int,
     *     'queryTimeout'?: int,
     *     'clientSessionKeepAlive'?: bool,
     *     'maxBackoffAttempts'?:int
     * } $params
     */
    public static function getConnection(
        string $host,
        string $user,
        string $password,
        array $params
    ): Connection {
        return DriverManager::getConnection(array_merge(
            $params,
            [
                'driverClass' => SnowflakeDriver::class,
                'host' => $host,
                'user' => $user,
                'password' => $password,
            ]
        ));
    }
}
