<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Schema\Exasol;

use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;

class ExasolSchemaQueryBuilder
{
    public function getCreateSchemaCommand(string $schemaName): string
    {
        return sprintf('CREATE SCHEMA %s', ExasolQuote::quoteSingleIdentifier($schemaName));
    }

    public function getDropSchemaCommand(string $schemaName): string
    {
        return sprintf('DROP SCHEMA %s CASCADE', ExasolQuote::quoteSingleIdentifier($schemaName));
    }
}
