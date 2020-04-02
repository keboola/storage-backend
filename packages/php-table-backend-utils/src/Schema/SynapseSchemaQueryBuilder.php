<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Schema;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;

class SynapseSchemaQueryBuilder
{
    /** @var SQLServer2012Platform|AbstractPlatform */
    private $platform;

    public function __construct(Connection $connection)
    {
        $this->platform = $connection->getDatabasePlatform();
    }

    public function getCreateSchemaCommand(string $schemaName): string
    {
        return sprintf('CREATE SCHEMA %s', $this->platform->quoteSingleIdentifier($schemaName));
    }

    public function getDropSchemaCommand(string $schemaName): string
    {
        return sprintf('DROP SCHEMA %s', $this->platform->quoteSingleIdentifier($schemaName));
    }
}
