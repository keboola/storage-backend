<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Schema;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Keboola\TableBackendUtils\Escaping\SynapseQuote;

class SynapseSchemaQueryBuilder
{
    public function getCreateSchemaCommand(string $schemaName): string
    {
        return sprintf('CREATE SCHEMA %s', SynapseQuote::quoteSingleIdentifier($schemaName));
    }

    public function getDropSchemaCommand(string $schemaName): string
    {
        return sprintf('DROP SCHEMA %s', SynapseQuote::quoteSingleIdentifier($schemaName));
    }
}
