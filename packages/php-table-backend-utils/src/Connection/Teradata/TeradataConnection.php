<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Teradata;

use Doctrine\DBAL\Connection;
use Exception;
use Throwable;

class TeradataConnection
{
    /**
     * @param array{
     *     host:string,
     *     user:string,
     *     password:string,
     *     port:int,
     *     dbname:?string,
     * } $params
     * @return Connection
     * @throws Exception
     */
    public static function getConnection(array $params): Connection
    {
        $params = array_merge($params, [
            'driverClass' => TeradataDriver::class,
        ]);

        try {
            return \Doctrine\DBAL\DriverManager::getConnection($params);
        } catch (Throwable $e) {
            throw new Exception($e->getMessage(), (int) $e->getCode(), $e);
        }
    }
}
