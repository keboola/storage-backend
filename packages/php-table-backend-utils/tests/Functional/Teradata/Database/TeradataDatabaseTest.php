<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Teradata\Database;

use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Tests\Keboola\TableBackendUtils\Functional\Teradata\TeradataBaseCase;

/**
 * @covers SynapseTableQueryBuilder
 */
class TeradataDatabaseTest extends TeradataBaseCase
{
    public const TEST_SCHEMA = self::TESTS_PREFIX . 'qb-schema';
    public const TEST_SCHEMA_2 = self::TESTS_PREFIX . 'qb-schema2';
    public const TEST_STAGING_TABLE = '#stagingTable';
    public const TEST_STAGING_TABLE_2 = '#stagingTable2';
    public const TEST_TABLE = self::TESTS_PREFIX . 'test';
    public const TEST_TABLE_2 = self::TESTS_PREFIX . 'test2';

    protected function setUp(): void
    {
        parent::setUp();
//        $this->dropAllWithinSchema(self::TEST_SCHEMA);
//        $this->dropAllWithinSchema(self::TEST_SCHEMA_2);
    }

    public function testGetDatabase(): void
    {
        $databaseName = $this->connection->executeQuery('SELECT DATABASE')->fetchOne();
        self::assertNotNull($databaseName);
    }
}
