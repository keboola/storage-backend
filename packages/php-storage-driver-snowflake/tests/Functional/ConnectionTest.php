<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional;

use PHPUnit\Framework\Attributes\DoesNotPerformAssertions;

class ConnectionTest extends BaseCase
{
    #[DoesNotPerformAssertions]
    public function testQuery(): void
    {
        $connection = $this->getSnowflakeConnection();
        $connection->executeQuery('SELECT 1');
    }
}
