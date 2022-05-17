<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Table\Teradata;

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
    private \Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder $qb;

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
    public function createTableInvalidPKsProvider(): \Generator
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
}
