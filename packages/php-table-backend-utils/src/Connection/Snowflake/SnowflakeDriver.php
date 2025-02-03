<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Snowflake;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Exception;
use Keboola\TableBackendUtils\Connection\Snowflake\Exception\PrivateKeyStringIsNotValid;

class SnowflakeDriver implements Driver
{
    private ?string $certFilePath = null;

    /**
     * @param array{
     *     'host':string,
     *     'user':string,
     *     'password'?:string,
     *     'privateKey'?:string,
     *     'port'?:string,
     *     'warehouse'?:string,
     *     'database'?:string,
     *     'schema'?:string,
     *     'tracing'?:int,
     *     'loginTimeout'?:int,
     *     'networkTimeout'?:int,
     *     'queryTimeout'?: int,
     *     'clientSessionKeepAlive'?: bool,
     *     'maxBackoffAttempts'?:int
     * } $params
     */
    public function connect(
        array $params,
    ): SnowflakeConnection {
        if (isset($params['privateKey']) && $params['privateKey'] !== '') {
            $this->certFilePath = $this->prepareAndSavePrivateKey($params['privateKey']);
            unset($params['privateKey']);
            $params['privateKeyPath'] = $this->certFilePath;
        }

        $dsn = SnowflakeDSNGenerator::generateDSN($params);

        return new SnowflakeConnection($dsn, $params['user'], $params['password'] ?? '', $params);
    }

    public function getDatabasePlatform(): SnowflakePlatform
    {
        return new SnowflakePlatform();
    }

    public function getSchemaManager(Connection $conn, AbstractPlatform $platform): SnowflakeSchemaManager
    {
        assert($platform instanceof SnowflakePlatform);
        return new SnowflakeSchemaManager($conn, $platform);
    }

    public function getExceptionConverter(): SnowflakeExceptionConverter
    {
        return new SnowflakeExceptionConverter();
    }

    private function prepareAndSavePrivateKey(string $privateKey): string
    {
        $privateKeyResource = openssl_pkey_get_private($privateKey);
        if (!$privateKeyResource) {
            throw new PrivateKeyStringIsNotValid();
        }

        $pemPKCS8 = '';
        openssl_pkey_export($privateKeyResource, $pemPKCS8);

        $privateKeyPath = tempnam(sys_get_temp_dir(), 'snowflake_private_key_' . uniqid()) . '.p8';
        file_put_contents($privateKeyPath, $pemPKCS8);

        return $privateKeyPath;
    }

    public function __destruct()
    {
        if ($this->certFilePath !== null && file_exists($this->certFilePath)) {
            unlink($this->certFilePath);
        }
    }
}
