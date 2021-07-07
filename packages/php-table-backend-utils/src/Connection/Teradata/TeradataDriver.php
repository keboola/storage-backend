<?php

namespace Keboola\TableBackendUtils\Connection\Teradata;

use Doctrine\DBAL\Driver\PDO;
use Doctrine\Deprecations\Deprecation;

class TeradataDriver extends AbstractTeradataDriver
{
    public function connect(array $params, $username = null, $password = null, array $driverOptions = [])
    {
        $odbcDSN = sprintf('DRIVER={Teradata};DBCName=%s;TDMSTPortNumber=%s;Charset=UTF8', $params['host'], $params['port']);

        $pdoDSN = "odbc:{$odbcDSN}";

        $pdo = new PDO\Connection($pdoDSN, $username, $password);
        return $pdo;
    }

    public function getName()
    {
        Deprecation::trigger(
            'doctrine/dbal',
            'https://github.com/doctrine/dbal/issues/3580',
            'Driver::getName() is deprecated'
        );

        return 'teradata';
    }
}
