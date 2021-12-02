<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Schema\Snowflake;

use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

class SnowflakeSchemaQueryBuilder
{
    public function getCreateSchemaCommand(string $schemaName): string
    {
        return sprintf('CREATE SCHEMA %s', SnowflakeQuote::quoteSingleIdentifier($schemaName));
    }

    public function getDropSchemaCommand(string $schemaName, bool $cascade = true): string
    {
        return sprintf(
            'DROP SCHEMA %s %s',
            SnowflakeQuote::quoteSingleIdentifier($schemaName),
            $cascade ? 'CASCADE' : 'RESTRICT'
        );
    }
}
