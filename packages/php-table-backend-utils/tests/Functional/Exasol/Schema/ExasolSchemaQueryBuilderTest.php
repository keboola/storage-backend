<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Exasol\Schema;

use Doctrine\DBAL\Exception;
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

        /** @var string[] $extracted */
        $extracted = DataHelper::extractByKey($schemas, 'SCHEMA_NAME');
        return $extracted;
    }

    public function testGetDropSchemaCommandWithCascade(): void
    {
        $qb = new ExasolSchemaQueryBuilder();
        // Creates schema self::TEST_SCHEMA with a table in it.
        // Drop column has CASCADE option true, so it should drop it anyway
        $this->initTable(self::TEST_SCHEMA);
        $this->connection->executeQuery($qb->getCreateSchemaCommand(self::TEST_SCHEMA_2));
        $schemas = $this->getSchemaFromDatabase();
        self::assertCount(1, $schemas);

        // drop testing schema leave schema2
        $this->connection->executeQuery($qb->getDropSchemaCommand(self::TEST_SCHEMA));
        $schemas = $this->getSchemaFromDatabase();
        self::assertEmpty($schemas);
    }

    public function testGetDropSchemaCommandWithRestrict(): void
    {
        // try to delete schema with table in it but with RESTRICT option
        $qb = new ExasolSchemaQueryBuilder();
        $this->initTable();
        $this->expectException(Exception::class);
        $this->connection->executeQuery($qb->getDropSchemaCommand(self::TEST_SCHEMA, false));
        $schemas = $this->getSchemaFromDatabase();
        self::assertNotEmpty($schemas);
    }
}
