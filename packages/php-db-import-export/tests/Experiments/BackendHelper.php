<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportExperiments;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Logging\Middleware;
use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeConnectionFactory;
use Tests\Keboola\Db\ImportExportFunctional\DebugLogger;

class BackendHelper
{
    public static function getSnowflakeConnection(): Connection
    {
        return SnowflakeConnectionFactory::getConnection(
            (string) getenv('SNOWFLAKE_HOST'),
            (string) getenv('SNOWFLAKE_USER'),
            (string) getenv('SNOWFLAKE_PASSWORD'),
            [
                'port' => (string) getenv('SNOWFLAKE_PORT'),
                'warehouse' => (string) getenv('SNOWFLAKE_WAREHOUSE'),
                'database' => (string) getenv('SNOWFLAKE_DATABASE'),
            ],
            self::getDoctrineLogger()
        );
    }

    public static function getDoctrineLogger(): Configuration
    {
        return (new Configuration())->setMiddlewares([new Middleware(new DebugLogger())]);
    }
}
