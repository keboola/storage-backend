<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Teradata;

use Doctrine\DBAL\Connection as DBALConnection;
use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Driver\API\ExceptionConverter;
use Doctrine\DBAL\Driver\PDO as PDODriver;
use Doctrine\DBAL\Driver\PDO\Exception;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Keboola\TableBackendUtils\Connection\ConnectionRetryWrapper;
use Keboola\TableBackendUtils\Connection\Snowflake\Exception\DriverException;
use PDO;
use PDOException;
use Retry\RetryProxy;

// TODO create abstract class as it is for others
class TeradataDriver implements Driver
{

    /**
     * @param string[] $params
     * @param string|null $username
     * @param string|null $password
     * @param string[] $driverOptions
     */
    // ignores type in $username and $password because it woudn't be compatible with interface
    // phpcs:ignore SlevomatCodingStandard.TypeHints.TypeHintDeclaration.MissingParameterTypeHint
    public function connect(
        array $params,
        $username = null,
        $password = null,
        array $driverOptions = []
    ): Connection {
        $dsn = sprintf(
            'DRIVER={Teradata};DBCName=%s;TDMSTPortNumber=%s;Charset=UTF8',
            $params['host'],
            $params['port']
        );

        return new ConnectionRetryWrapper(
            new TeradataConnectionWrapper($dsn, $params['user'] ?? '', $params['password'] ?? ''),
            new RetryProxy(
                new TeradataRetryPolicy()
            )
        );
    }

    public function getDatabasePlatform(): TeradataPlatform
    {
        return new TeradataPlatform();
    }

    public function getSchemaManager(DBALConnection $conn, AbstractPlatform $platform): TeradataSchemaManager
    {
        assert($platform instanceof TeradataPlatform);
        return new TeradataSchemaManager($conn, $platform);
    }

    public function getExceptionConverter(): ExceptionConverter
    {
        return new TeradataExceptionConverter();
    }
}
