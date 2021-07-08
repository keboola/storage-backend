<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Teradata;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Driver\PDO;
use Doctrine\Deprecations\Deprecation;

// TODO create abstract class as it is for others
class TeradataDriver implements Driver
{

    /**
     * @param string[] $params
     * @param string|null $username
     * @param string|null $password
     * @param string[] $driverOptions
     * @return PDO\Connection
     */
    // ignores type in $username and $password because it woudn't be compatible with interface
    // phpcs:ignore SlevomatCodingStandard.TypeHints.TypeHintDeclaration.MissingParameterTypeHint
    public function connect(
        array $params,
        $username = null,
        $password = null,
        array $driverOptions = []
    ): PDO\Connection {
        $odbcDSN = sprintf(
            'DRIVER={Teradata};DBCName=%s;TDMSTPortNumber=%s;Charset=UTF8',
            $params['host'],
            $params['port']
        );

        $pdoDSN = "odbc:{$odbcDSN}";

        $pdo = new PDO\Connection($pdoDSN, $username, $password);
        return $pdo;
    }

    public function getName(): string
    {
        Deprecation::trigger(
            'doctrine/dbal',
            'https://github.com/doctrine/dbal/issues/3580',
            'Driver::getName() is deprecated'
        );

        return 'teradata';
    }

    public function getDatabasePlatform(): TeradataPlatform
    {
        return new TeradataPlatform();
    }

    public function getSchemaManager(Connection $conn): TeradataSchemaManager
    {
        return new TeradataSchemaManager($conn);
    }

    /**
     * {@inheritdoc}
     *
     * @deprecated Use Connection::getDatabase() instead.
     */
    public function getDatabase(Connection $conn)
    {
        $params = $conn->getParams();

        if (isset($params['dbname'])) {
            return $params['dbname'];
        }

        $database = $conn->executeQuery('SELECT DATABASE')->fetchOne();

        assert($database !== false);

        return $database;
    }
}
