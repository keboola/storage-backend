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
    /**
     * Check if a string is a valid RSA private key
     */
    private static function isValidRsaPrivateKey(string $key): bool
    {
        // Remove any whitespace and check if it looks like a PEM encoded key
        $key = trim($key);
        if (!str_contains($key, '-----BEGIN') || !str_contains($key, 'PRIVATE KEY-----')) {
            return false;
        }

        // Try to get the private key details
        $privateKey = openssl_pkey_get_private($key);
        if ($privateKey === false) {
            return false;
        }

        // Get the details to verify it's an RSA key
        $details = openssl_pkey_get_details($privateKey);

        // Check if it's an RSA key
        return $details !== false && isset($details['key']) && $details['type'] === OPENSSL_KEYTYPE_RSA;
    }

    public static function createFromCredentials(GenericBackendCredentials $credentials): Connection
    {
        $meta = $credentials->getMeta();
        if ($meta !== null) {
            $meta = $meta->unpack();
            assert($meta instanceof SnowflakeCredentialsMeta);
        } else {
            throw new Exception('SnowflakeCredentialsMeta is required.');
        }

        // Check if the secret is a valid RSA private key
        $isRsaKey = self::isValidRsaPrivateKey($credentials->getSecret());

        $connectionParams = [
            'port' => (string) $credentials->getPort(),
            'warehouse' => $meta->getWarehouse(),
            'database' => $meta->getDatabase(),
        ];

        if ($isRsaKey) {
            return SnowflakeConnectionFactory::getConnectionWithCert(
                $credentials->getHost(),
                $credentials->getPrincipal(),
                $credentials->getSecret(),
                $connectionParams,
            );
        }

        return SnowflakeConnectionFactory::getConnection(
            $credentials->getHost(),
            $credentials->getPrincipal(),
            $credentials->getSecret(),
            $connectionParams,
        );
    }
}
