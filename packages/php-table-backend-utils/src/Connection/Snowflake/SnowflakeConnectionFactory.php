<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Snowflake;

use Doctrine\DBAL\Configuration;
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
        ?string $password,
        ?string $privateKey,
        array $params,
        ?Configuration $config = null,
    ): Connection {

        /** @var array{
         *     'port'?:string,
         *     'warehouse'?:string,
         *     'database'?:string,
         *     'schema'?:string,
         *     'tracing'?:int,
         *     'loginTimeout'?:int,
         *     'networkTimeout'?:int,
         *     'queryTimeout'?: int,
         *     'clientSessionKeepAlive'?: bool,
         *     'maxBackoffAttempts'?:int,
         *     'driverClass': class-string<Doctrine\DBAL\Driver>,
         *     'host': string,
         *     'user': string,
         *     'password'?:string,
         *     'privateKey'?:string,
         * } $params */
        $params = array_merge(
            $params,
            [
                'driverClass' => SnowflakeDriver::class,
                'host' => $host,
                'user' => $user,
            ],
        );

        if ($password !== null && $password !== '') {
            $params = array_merge(
                $params,
                [
                    'password' => $password,
                ],
            );
        }

        if ($privateKey !== null && $privateKey !== '') {
            $params = array_merge(
                $params,
                [
                    'privateKey' => $privateKey,
                ],
            );
        }

        return DriverManager::getConnection(
            $params,
            $config,
        );
    }
}
