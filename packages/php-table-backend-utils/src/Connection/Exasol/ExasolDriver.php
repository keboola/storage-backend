<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Exasol;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\OraclePlatform;
use Doctrine\DBAL\Schema\OracleSchemaManager;

class ExasolDriver implements Driver
{
    /**
     * @param array{
     *     'host':string,
     *     'user':string,
     *     'password':string,
     *     'skipCertCheck':bool,
     * } $params
     */
    public function connect(
        array $params
    ): ExasolConnection {
        assert(array_key_exists('host', $params));
        assert(array_key_exists('user', $params));
        assert(array_key_exists('password', $params));
        $dsn = 'odbc:Driver=exasol;EXAHOST=' . $params['host'];

        if ($params['skipCertCheck']) {
            $dsn .= ';FINGERPRINT=NoCertCheck;';
        }

        return new ExasolConnection($dsn, $params['user'], $params['password'], $params);
    }

    public function getDatabasePlatform(): OraclePlatform
    {
        return new OraclePlatform();
    }

    public function getSchemaManager(Connection $conn, AbstractPlatform $platform): OracleSchemaManager
    {
        assert($platform instanceof OraclePlatform);
        return new OracleSchemaManager($conn, $platform);
    }

    public function getExceptionConverter(): Driver\API\OCI\ExceptionConverter
    {
        return new Driver\API\OCI\ExceptionConverter();
    }
}
