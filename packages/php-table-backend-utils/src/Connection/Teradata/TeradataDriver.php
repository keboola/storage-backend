<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Teradata;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Driver\API\ExceptionConverter;
use Doctrine\DBAL\Driver\PDO as PDODriver;
use Doctrine\DBAL\Driver\PDO\Exception;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use PDO;
use PDOException;

// TODO create abstract class as it is for others
class TeradataDriver implements Driver
{

    /**
     * @param string[] $params
     * @param string|null $username
     * @param string|null $password
     * @param string[] $driverOptions
     * @return PDODriver\Connection
     */
    // ignores type in $username and $password because it woudn't be compatible with interface
    // phpcs:ignore SlevomatCodingStandard.TypeHints.TypeHintDeclaration.MissingParameterTypeHint
    public function connect(
        array $params,
        $username = null,
        $password = null,
        array $driverOptions = []
    ): PDODriver\Connection {
        $odbcDSN = sprintf(
            'DRIVER={Teradata};DBCName=%s;TDMSTPortNumber=%s;Charset=UTF8',
            $params['host'],
            $params['port']
        );

        $pdoDSN = "odbc:{$odbcDSN}";

        try {
            $pdo = new PDO(
                $pdoDSN,
                $params['user'] ?? '',
                $params['password'] ?? '',
                $driverOptions
            );
        } catch (PDOException $exception) {
            throw Exception::new($exception);
        }

        return new PDODriver\Connection($pdo);
    }

    public function getDatabasePlatform(): TeradataPlatform
    {
        return new TeradataPlatform();
    }

    public function getSchemaManager(Connection $conn, AbstractPlatform $platform): TeradataSchemaManager
    {
        assert($platform instanceof TeradataPlatform);
        return new TeradataSchemaManager($conn, $platform);
    }

    public function getExceptionConverter(): ExceptionConverter
    {
        return new TeradataExceptionConverter();
    }
}
