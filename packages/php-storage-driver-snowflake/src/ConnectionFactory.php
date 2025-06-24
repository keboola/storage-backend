<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials\SnowflakeCredentialsMeta;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeConnectionFactory;

final class ConnectionFactory
{
    public static function createFromCredentials(GenericBackendCredentials $credentials): Connection
    {
        $meta = $credentials->getMeta();
        if ($meta !== null) {
            $meta = $meta->unpack();
            assert($meta instanceof SnowflakeCredentialsMeta);
        } else {
            throw new Exception('SnowflakeCredentialsMeta is required.');
        }

        return SnowflakeConnectionFactory::getConnection(
            $credentials->getHost(),
            $credentials->getPrincipal(),
            $credentials->getSecret(),
            [
                'port' => (string) $credentials->getPort(),
                'warehouse' => $meta->getWarehouse(),
                'database' => $meta->getDatabase(),
            ],
        );
    }
}
