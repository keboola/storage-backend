<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\Shared\NameGenerator;

use Keboola\StorageDriver\Shared\NameGenerator\SynapseNameGenerator;

class SynapseNameGeneratorTest extends GenericNameGeneratorTest
{
    public function testCreateGlobalSchemaOwner(): void
    {
        $generator = new SynapseNameGenerator(self::TEST_CLIENT_DB_PREFIX);
        $objectName = $generator->createGlobalSchemaOwner();
        $this->assertSame('MY_TEST_PREFIX_GLOBAL_SCHEMA_OWNER_ROLE', $objectName);
    }
}
