<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Schema\Snowflake;

use Doctrine\DBAL\Exception;
use Keboola\TableBackendUtils\DataHelper;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Schema\Snowflake\SnowflakeSchemaQueryBuilder;
use Tests\Keboola\TableBackendUtils\Functional\Connection\Snowflake\SnowflakeBaseCase;

/**
 * @covers SnowflakeSchemaQueryBuilder
 */
class SnowflakeSchemaQueryBuilderTest extends SnowflakeBaseCase
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
        $qb = new SnowflakeSchemaQueryBuilder();
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
                'SHOW SCHEMAS'
        );

        return DataHelper::extractByKey($schemas, 'name');
    }

    public function testGetDropSchemaCommandWithCascade(): void
    {
        $qb = new SnowflakeSchemaQueryBuilder();
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
        $qb = new SnowflakeSchemaQueryBuilder();
        $this->initTable();
        $this->expectException(Exception::class);
        $this->connection->executeQuery($qb->getDropSchemaCommand(self::TEST_SCHEMA, false));
        $schemas = $this->getSchemaFromDatabase();
        self::assertNotEmpty($schemas);
    }
}
