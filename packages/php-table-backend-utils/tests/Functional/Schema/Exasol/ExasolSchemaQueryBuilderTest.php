<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Schema\Exasol;

use Keboola\TableBackendUtils\DataHelper;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Schema\Exasol\ExasolSchemaQueryBuilder;
use Tests\Keboola\TableBackendUtils\Functional\Exasol\ExasolBaseCase;

/**
 * @covers ExasolSchemaQueryBuilder
 */
class ExasolSchemaQueryBuilderTest extends ExasolBaseCase
{
    public const TEST_SCHEMA = self::TESTS_PREFIX . 'qb-schema-schema';
    public const TEST_SCHEMA_2 = self::TESTS_PREFIX . 'qb-schema-schema2';

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanSchema(self::TEST_SCHEMA);
        $this->cleanSchema(self::TEST_SCHEMA_2);
    }

    public function testGetCreateSchemaCommand(): void
    {
        $qb = new ExasolSchemaQueryBuilder();
        $schemas = $this->getSchemaFromDatabase();
        self::assertEmpty($schemas);

        $this->connection->executeQuery($qb->getCreateSchemaCommand(self::TEST_SCHEMA));
        $this->connection->executeQuery($qb->getCreateSchemaCommand(self::TEST_SCHEMA_2));

        $schemas = $this->getSchemaFromDatabase();
        self::assertCount(1, $schemas);
        self::assertSame([self::TEST_SCHEMA], $schemas);
    }

    /**
     * @return string[]
     */
    private function getSchemaFromDatabase(): array
    {
        $schemas = $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT "SCHEMA_NAME" FROM "SYS"."EXA_ALL_SCHEMAS" WHERE "SCHEMA_NAME" = %s',
                ExasolQuote::quote(self::TEST_SCHEMA)
            )
        );

        return DataHelper::extractByKey($schemas, 'SCHEMA_NAME');
    }

    public function testGetDropSchemaCommand(): void
    {
        $qb = new ExasolSchemaQueryBuilder();

        $this->connection->executeQuery($qb->getCreateSchemaCommand(self::TEST_SCHEMA));
        $this->connection->executeQuery($qb->getCreateSchemaCommand(self::TEST_SCHEMA_2));
        $schemas = $this->getSchemaFromDatabase();
        self::assertCount(1, $schemas);

        // drop testing schema leave schema2
        $this->connection->executeQuery($qb->getDropSchemaCommand(self::TEST_SCHEMA));
        $schemas = $this->getSchemaFromDatabase();
        self::assertEmpty($schemas);
    }
}
