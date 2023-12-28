<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Synapse\Schema;

use Keboola\TableBackendUtils\Schema\SynapseSchemaQueryBuilder;
use Tests\Keboola\TableBackendUtils\Functional\Synapse\SynapseBaseCase;

/**
 * @covers SynapseSchemaQueryBuilder
 */
class SynapseSchemaQueryBuilderTest extends SynapseBaseCase
{
    public const TEST_SCHEMA = self::TESTS_PREFIX . 'qb-schema-schema';
    public const TEST_SCHEMA_2 = self::TESTS_PREFIX . 'qb-schema-schema2';

    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema(self::TEST_SCHEMA);
        $this->dropAllWithinSchema(self::TEST_SCHEMA_2);
    }

    public function testGetCreateSchemaCommand(): void
    {
        $qb = new SynapseSchemaQueryBuilder();
        $schemas = $this->getSchemaFromDatabase();
        $this->assertEmpty($schemas);

        $this->connection->executeStatement($qb->getCreateSchemaCommand(self::TEST_SCHEMA));
        $this->connection->executeStatement($qb->getCreateSchemaCommand(self::TEST_SCHEMA_2));

        $schemas = $this->getSchemaFromDatabase();
        $this->assertCount(1, $schemas);
        $this->assertSame([self::TEST_SCHEMA], $schemas);
    }

    /**
     * @return array<int,string>
     */
    private function getSchemaFromDatabase(): array
    {
        /** @var array{array{name:string}} $schemas */
        $schemas = $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT name FROM sys.schemas WHERE name = \'%s\'',
                self::TEST_SCHEMA,
            ),
        );

        return array_map(static fn(array $schema) => $schema['name'], $schemas);
    }

    public function testGetDropSchemaCommand(): void
    {
        $qb = new SynapseSchemaQueryBuilder();

        $this->connection->executeStatement($qb->getCreateSchemaCommand(self::TEST_SCHEMA));
        $this->connection->executeStatement($qb->getCreateSchemaCommand(self::TEST_SCHEMA_2));
        $schemas = $this->getSchemaFromDatabase();
        $this->assertCount(1, $schemas);

        // drop testing schema leave schema2
        $this->connection->executeStatement($qb->getDropSchemaCommand(self::TEST_SCHEMA));
        $schemas = $this->getSchemaFromDatabase();
        $this->assertEmpty($schemas);
    }
}
