<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Table\Teradata;

use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Tests\Keboola\TableBackendUtils\Functional\Teradata\TeradataBaseCase;

/**
 * @covers SynapseTableReflection
 * @uses   ColumnCollection
 */
class TeradataTableReflectionTest extends TeradataBaseCase
{
    public const TEST_DATABASE = self::TESTS_PREFIX . 'refTableDatabase';
    // tables
    public const TABLE_GENERIC = self::TESTS_PREFIX . 'refTab';
    //views
    public const VIEW_GENERIC = self::TESTS_PREFIX . 'refview';

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanDatabase(self::TEST_DATABASE);
        $this->createDatabase(self::TEST_DATABASE);
    }

    public function testGetTableColumnsNames(): void
    {
        $this->initTable();
        $ref = new TeradataTableReflection($this->connection, self::TEST_DATABASE, self::TABLE_GENERIC);

        self::assertSame([
            'id',
            'first_name',
            'last_name',
        ], $ref->getColumnsNames());
    }

    protected function initTable(
        string $database = self::TEST_DATABASE,
        string $table = self::TABLE_GENERIC
    ): void {
        $this->connection->executeQuery(
            sprintf(
                'CREATE MULTISET TABLE %s.%s ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      "id" INTEGER,
      "first_name" VARCHAR(100),
      "last_name" VARCHAR(100)
     );',
                TeradataQuote::quoteSingleIdentifier($database),
                TeradataQuote::quoteSingleIdentifier($table)
            )
        );
    }

    public function testGetPrimaryKeysNames(): void
    {
//        TODO
    }

    public function testGetRowsCount(): void
    {
        //        TODO
    }
}
