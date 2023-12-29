<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Snowflake\Schema;

use Doctrine\DBAL\Exception;
use Keboola\TableBackendUtils\DataHelper;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Schema\Snowflake\SnowflakeSchemaQueryBuilder;
use Tests\Keboola\TableBackendUtils\Functional\Snowflake\SnowflakeBaseCase;

/**
 * @covers SnowflakeSchemaQueryBuilder
 */
class SnowflakeSchemaQueryBuilderTest extends SnowflakeBaseCase
{
    public const TEST_SCHEMA = self::TESTS_PREFIX . 'qb_schema_schema';
    public const TEST_SCHEMA_2 = self::TESTS_PREFIX . 'qb_schema_schema2';

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanSchema(self::TEST_SCHEMA);
        $this->cleanSchema(self::TEST_SCHEMA_2);
    }

    public function testGetCreateSchemaCommand(): void
    {
        $qb = new SnowflakeSchemaQueryBuilder();
        $schemasNumberBefore = count($this->getSchemaFromDatabase());

        $this->connection->executeQuery($qb->getCreateSchemaCommand(self::TEST_SCHEMA));

        $schemas = $this->getSchemaFromDatabase();
        self::assertCount($schemasNumberBefore + 1, $schemas);
        self::assertContains(self::TEST_SCHEMA, $schemas);
    }

    /**
     * @return string[]
     */
    private function getSchemaFromDatabase(): array
    {
        /** @var array<array{name:string}> $schemas */
        $schemas = $this->connection->fetchAllAssociative(
            'SHOW SCHEMAS',
        );

        return array_map(static fn(array $schema) => trim($schema['name']), $schemas);
    }

    public function testGetDropSchemaCommandWithCascade(): void
    {
        $schemasNumberBefore = count($this->getSchemaFromDatabase());
        $this->cleanSchema(self::TEST_SCHEMA_2);

        $qb = new SnowflakeSchemaQueryBuilder();
        // Creates schema self::TEST_SCHEMA with a table in it. initTable() creates it.
        // DROP SCHEMA dooesn't care about objects in it, so it should drop it anyway
        $this->initTable(self::TEST_SCHEMA_2);
        $schemas = $this->getSchemaFromDatabase();
        self::assertCount($schemasNumberBefore + 1, $schemas);
        self::assertContains(self::TEST_SCHEMA_2, $schemas);

        $this->connection->executeQuery($qb->getDropSchemaCommand(self::TEST_SCHEMA_2));
        $schemas = $this->getSchemaFromDatabase();
        self::assertCount($schemasNumberBefore, $schemas);
        self::assertNotContains(self::TEST_SCHEMA_2, $schemas);
    }

    public function testGetDropSchemaCommandWithRestrict(): void
    {
        // try to delete schema with table in it but with RESTRICT option.
        // It should DROP it anyway, because table hasn't any FKs
        $qb = new SnowflakeSchemaQueryBuilder();
        $schemasNumberBefore = count($this->getSchemaFromDatabase());

        $this->initTable(self::TEST_SCHEMA);
        $this->connection->executeQuery($qb->getDropSchemaCommand(self::TEST_SCHEMA, false));
        $schemas = $this->getSchemaFromDatabase();
        self::assertNotEmpty($schemas);
        self::assertCount($schemasNumberBefore, $this->getSchemaFromDatabase());
    }

    public function testGetDropSchemaCommandWithRestrictWithFK(): void
    {
        $qb = new SnowflakeSchemaQueryBuilder();
        $schemasNumberBefore = count($this->getSchemaFromDatabase());

        $this->initTable(self::TEST_SCHEMA, 'users');

        // create table with FK to another table -> DROP SCHEMA RESTRICT should fail
        $this->connection->executeQuery(
            sprintf(
                '
                ALTER TABLE  %s.%s ADD PRIMARY KEY ("id");',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                SnowflakeQuote::quoteSingleIdentifier('users'),
            ),
        );
        $this->connection->executeQuery(
            sprintf(
                '
CREATE OR REPLACE TABLE %s.%s (
    "id" INTEGER,
    "id_user" INTEGER,
    "role" VARCHAR(100),
    CONSTRAINT "fkey_1" FOREIGN KEY ("id_user") REFERENCES "users" ("id")
);',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                SnowflakeQuote::quoteSingleIdentifier('roles'),
            ),
        );

        try {
            $this->connection->executeQuery($qb->getDropSchemaCommand(self::TEST_SCHEMA, false));
            self::fail('Should fail');
        } catch (Exception $e) {
            self::assertStringContainsString('Cannot drop the schema because of dependencies', $e->getMessage());
        }
        self::assertCount($schemasNumberBefore + 1, $this->getSchemaFromDatabase());

        // DROP FK -> DROP SCHEMA should be ok
        $this->connection->executeQuery(
            sprintf(
                '
                ALTER TABLE  %s.%s DROP FOREIGN KEY ("id_user");',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                SnowflakeQuote::quoteSingleIdentifier('roles'),
            ),
        );

        $this->connection->executeQuery($qb->getDropSchemaCommand(self::TEST_SCHEMA, false));
        self::assertCount($schemasNumberBefore, $this->getSchemaFromDatabase());
    }
}
