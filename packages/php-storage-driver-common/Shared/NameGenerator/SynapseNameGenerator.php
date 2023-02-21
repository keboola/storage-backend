<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\NameGenerator;

class SynapseNameGenerator extends GenericNameGenerator
{
    public function createGlobalSchemaOwner(): string
    {
        return strtoupper(
            $this->stackPrefix
            . 'GLOBAL_SCHEMA_OWNER'
            . self::ROLE_SUFFIX
        );
    }
}
