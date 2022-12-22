<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Table\Teradata;

use Generator;
use Keboola\Datatype\Definition\Teradata;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\QueryBuilderException;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use PHPUnit\Framework\TestCase;

/**
 * @covers TeradataTableQueryBuilder
 * @uses   ColumnCollection
 */
class TeradataTableQueryBuilderTest extends TestCase
{
    private TeradataTableQueryBuilder $qb;

    public function setUp(): void
    {
        $this->qb = new TeradataTableQueryBuilder();
    }

    /**
     * @param TeradataColumn[] $columns
     * @param string[] $PKs
     * @dataProvider createTableInvalidPKsProvider
     * @throws \Exception
     */
    public function testGetCreateCommandWithInvalidPks(array $columns, array $PKs, string $exceptionString): void
    {
        $this->expectException(QueryBuilderException::class);
        $this->expectExceptionMessage($exceptionString);
        $this->qb->getCreateTableCommand('testDb', 'testTable', new ColumnCollection($columns), $PKs);
        self::fail('Should fail because of invalid PKs');
    }

    /**
     * @return \Generator<string, mixed, mixed, mixed>
     */
    public function createTableInvalidPKsProvider(): Generator
    {
        yield 'key of ouf columns' => [
            'cols' => [
                TeradataColumn::createGenericColumn('col1'),
                TeradataColumn::createGenericColumn('col2'),
            ],
            'primaryKeys' => ['colNotExisting'],
            'exceptionString' => 'Trying to set colNotExisting as PKs but not present in columns',
        ];
        yield 'pk on disallowed type' => [
            'cols' => [
                new TeradataColumn('col1', new Teradata(Teradata::TYPE_BLOB, ['nullable' => false])),
            ],
            'primaryKeys' => ['col1'],
            'exceptionString' => 'Trying to set PK on column col1 but type BLOB is not supported for PK',
        ];
        yield 'pk on nullable type' => [
            'cols' => [
                new TeradataColumn('col1', new Teradata(Teradata::TYPE_VARCHAR, ['nullable' => true])),
            ],
            'primaryKeys' => ['col1'],
            'exceptionString' => 'Trying to set PK on column col1 but this column is nullable',
        ];
    }


    public function testGetPKCommand(): void
    {
        $this->assertEquals(
            'ALTER TABLE "myDB"."myTable" ADD CONSTRAINT kbc_pk PRIMARY KEY ("my","rules");',
            $this->qb->getAddPrimaryKeyCommand('myDB', 'myTable', ['my', 'rules'])
        );
    }

    public function testGetDropPKCommand(): void
    {
        $this->assertEquals(
            'ALTER TABLE "myDB"."myTable" DROP CONSTRAINT kbc_pk;',
            $this->qb->getDropPrimaryKeyCommand('myDB', 'myTable')
        );
    }

    public function testGetDuplicationCommand(): void
    {
        $this->assertEquals(
            'SELECT MAX("_row_number_") AS "count" FROM
(
    SELECT ROW_NUMBER() OVER (PARTITION BY "my","rules" ORDER BY "my","rules") AS "_row_number_" FROM "myDB"."myTable"
) "data"',
            $this->qb->getCommandForDuplicates('myDB', 'myTable', ['my', 'rules'])
        );
    }
}
