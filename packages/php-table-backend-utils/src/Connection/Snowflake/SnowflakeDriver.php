<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Snowflake;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Platforms\AbstractPlatform;

class SnowflakeDriver implements Driver
{
    /**
     * @param array{
     *     'host':string,
     *     'user':string,
     *     'password':string,
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
    public function connect(
        array $params,
    ): SnowflakeConnection {
        $dsn = SnowflakeDSNGenerator::generateDSN($params);

        return new SnowflakeConnection($dsn, $params['user'], $params['password'], $params);
    }

    public function getDatabasePlatform(): SnowflakePlatform
    {
        return new SnowflakePlatform();
    }

    public function getSchemaManager(Connection $conn, AbstractPlatform $platform): SnowflakeSchemaManager
    {
        assert($platform instanceof SnowflakePlatform);
        return new SnowflakeSchemaManager($conn, $platform);
    }

    public function getExceptionConverter(): SnowflakeExceptionConverter
    {
        return new SnowflakeExceptionConverter();
    }
}
