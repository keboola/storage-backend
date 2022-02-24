<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Exasol;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;

class ExasolConnectionFactory
{
    public static function getConnection(
        string $host,
        string $user,
        string $password,
        ?Configuration $config = null
    ): Connection {
        return DriverManager::getConnection(
            [
                'driverClass' => ExasolDriver::class,
                'host' => $host,
                'user' => $user,
                'password' => $password,
            ],
            $config
        );
    }
}
