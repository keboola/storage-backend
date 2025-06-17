<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional;

use PHPUnit\Framework\TestCase;
use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeConnectionFactory;

abstract class BaseCase extends TestCase
{
    protected function getSnowflakeConnection(): Connection
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
        );
    }
}
