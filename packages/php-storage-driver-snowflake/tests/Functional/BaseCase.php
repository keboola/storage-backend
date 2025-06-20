<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeConnectionFactory;
use Keboola\TableBackendUtils\Schema\Snowflake\SnowflakeSchemaQueryBuilder;
use PHPUnit\Framework\TestCase;

abstract class BaseCase extends TestCase
{
    protected const SCHEMA_NAME = 'profiling';

    protected Connection $connection;

    protected function getSnowflakeConnection(): Connection
    {
        $this->connection = SnowflakeConnectionFactory::getConnection(
            (string) getenv('SNOWFLAKE_HOST'),
            (string) getenv('SNOWFLAKE_USER'),
            (string) getenv('SNOWFLAKE_PASSWORD'),
            [
                'port' => (string) getenv('SNOWFLAKE_PORT'),
                'warehouse' => (string) getenv('SNOWFLAKE_WAREHOUSE'),
                'database' => (string) getenv('SNOWFLAKE_DATABASE'),
            ],
        );

        return $this->connection;
    }

    protected function setUp(): void
    {
        $this->getSnowflakeConnection();

        $this->connection->executeQuery(
            (new SnowflakeSchemaQueryBuilder())->getCreateSchemaCommand(self::SCHEMA_NAME),
        );
    }

    protected function tearDown(): void
    {
        $this->connection->executeQuery(
            (new SnowflakeSchemaQueryBuilder())->getDropSchemaCommand(self::SCHEMA_NAME),
        );
    }
}
