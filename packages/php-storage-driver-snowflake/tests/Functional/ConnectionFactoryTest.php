<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional;

use Google\Protobuf\Any;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials\SnowflakeCredentialsMeta;
use Keboola\StorageDriver\Snowflake\ConnectionFactory;
use PHPUnit\Framework\TestCase;

class ConnectionFactoryTest extends TestCase
{
    public function testCreateFromCredentialsWithPrivateKey(): void
    {
        // Create credentials with a key
        $credentials = new GenericBackendCredentials();
        $credentials->setHost((string) getenv('SNOWFLAKE_HOST'));
        $credentials->setPrincipal((string) getenv('SNOWFLAKE_USER'));
        $credentials->setSecret((string) getenv('SNOWFLAKE_PRIVATE_KEY'));
        $credentials->setPort((int) getenv('SNOWFLAKE_PORT'));

        $meta = new Any();
        $meta->pack(
            (new SnowflakeCredentialsMeta())
                ->setWarehouse((string) getenv('SNOWFLAKE_WAREHOUSE'))
                ->setDatabase((string) getenv('SNOWFLAKE_DATABASE')),
        );
        $credentials->setMeta($meta);

        // Create connection
        $connection = ConnectionFactory::createFromCredentials($credentials);

        // Test connection works
        $result = $connection->executeQuery('SELECT 1 as TEST');
        $this->assertEquals(1, $result->fetchOne());
    }
}
