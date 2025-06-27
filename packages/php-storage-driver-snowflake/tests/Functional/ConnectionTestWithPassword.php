<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeConnectionFactory;

class ConnectionTestWithPassword extends BaseCase
{
    /**
     * @doesNotPerformAssertions
     */
    public function testQuery(): void
    {
        $connection = $this->getSnowflakeConnection();
        $connection->executeQuery('SELECT 1');
    }

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
}
