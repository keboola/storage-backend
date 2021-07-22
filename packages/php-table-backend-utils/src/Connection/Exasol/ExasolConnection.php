<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Exasol;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\OCI8\Driver;
use Doctrine\DBAL\DriverManager;
use Exception;
use PDO;
use PDOException;
use Throwable;

class ExasolConnection
{
    public static function getConnection(string $dsn, string $user, string $password): Connection
    {
        try {
            $dbh = new PDO('odbc:Driver=exasol;EXAHOST=' . $dsn, $user, $password);
        } catch (PDOException $e) {
            throw new Exception('Connection failed: ' . $e->getMessage(), (int) $e->getCode(), $e);
        }

        try {
            return DriverManager::getConnection([
                "pdo" => $dbh,
                "driverClass" => Driver::class,
            ]);
        } catch (Throwable $e) {
            throw new Exception($e->getMessage(), (int) $e->getCode(), $e);
        }
    }
}
