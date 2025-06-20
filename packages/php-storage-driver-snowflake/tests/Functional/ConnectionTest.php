<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional;

class ConnectionTest extends BaseCase
{
    /**
     * @doesNotPerformAssertions
     */
    public function testQuery(): void
    {
        $connection = $this->getSnowflakeConnection();
        $connection->executeQuery('SELECT 1');
    }
}
