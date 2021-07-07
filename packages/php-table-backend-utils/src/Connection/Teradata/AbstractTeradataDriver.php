<?php

namespace Keboola\TableBackendUtils\Connection\Teradata;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Driver\DriverException;
use Doctrine\DBAL\VersionAwarePlatformDriver;

abstract class AbstractTeradataDriver implements Driver, VersionAwarePlatformDriver
{
    public function getDatabasePlatform()
    {
        // TODO: Implement getDatabasePlatform() method.
    }

    public function getSchemaManager(Connection $conn)
    {
        // TODO: Implement getSchemaManager() method.
    }

    public function getDatabase(Connection $conn)
    {
        // TODO: Implement getDatabase() method.
    }

    public function createDatabasePlatformForVersion($version)
    {
        // TODO: Implement createDatabasePlatformForVersion() method.
    }

    public function convertException($message, DriverException $exception)
    {
        // TODO: Implement convertException() method.
    }
}
