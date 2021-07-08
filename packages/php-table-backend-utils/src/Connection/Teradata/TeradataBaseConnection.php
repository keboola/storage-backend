<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Teradata;

use Doctrine\DBAL\Connection;
use Exception;
use Throwable;

class TeradataBaseConnection
{
    /**
     * @param array<string, string|false> $params
     * @return Connection
     * @throws Exception
     */
    public static function getBaseConnection(array $params): Connection
    {
        $params = array_merge($params, [
            'port' => getenv('TERADATA_PORT'),
            'driverClass' => TeradataDriver::class,
        ]);

        try {
            return \Doctrine\DBAL\DriverManager::getConnection($params);
        } catch (Throwable $e) {
            throw new Exception($e->getMessage(), (int) $e->getCode(), $e);
        }
    }
}
