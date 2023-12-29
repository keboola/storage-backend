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

    /**
     * SNFLK doesn't care if there are objects in SCHEMA. It DROPs it anyway with the tables, even with RESTRICT flag
     * The RESTRICT flag is applied on FK constraints, not on objects in schema!
     */
    public function getDropSchemaCommand(string $schemaName, bool $cascade = true): string
    {
        return sprintf(
            'DROP SCHEMA %s %s',
            SnowflakeQuote::quoteSingleIdentifier($schemaName),
            $cascade ? 'CASCADE' : 'RESTRICT',
        );
    }
}
