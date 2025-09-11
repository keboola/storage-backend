<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional;

use Keboola\StorageDriver\Snowflake\ConnectionFactory;
use PHPUnit\Framework\TestCase;

class ConnectionFactoryTest extends TestCase
{
    use SnowflakeCredentialsTrait;

    public function testCreateFromCredentialsWithPassword(): void
    {
        // Create connection
        $connection = ConnectionFactory::createFromCredentials($this->createCredentialsWithPassword());

        // Test connection works
        $result = $connection->executeQuery('SELECT 1 as TEST');
        $this->assertEquals(1, $result->fetchOne());
    }

    public function testCreateFromCredentialsWithPrivateKey(): void
    {
        // Create connection
        $connection = ConnectionFactory::createFromCredentials($this->createCredentialsWithKeyPair());

        // Test connection works
        $result = $connection->executeQuery('SELECT 1 as TEST');
        $this->assertEquals(1, $result->fetchOne());
    }
}
