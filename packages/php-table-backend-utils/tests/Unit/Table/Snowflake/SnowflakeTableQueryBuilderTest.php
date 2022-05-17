<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Table\Snowflake;

use Generator;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\QueryBuilderException;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;
use PHPUnit\Framework\TestCase;

/**
 * @covers SnowflakeTableQueryBuilder
 * @uses   ColumnCollection
 */
class SnowflakeTableQueryBuilderTest extends TestCase
{
    private SnowflakeTableQueryBuilder $qb;

    public function setUp(): void
    {
        $this->qb = new SnowflakeTableQueryBuilder();
    }

    /**
     * @param SnowflakeColumn[] $columns
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
                SnowflakeColumn::createGenericColumn('col1'),
                SnowflakeColumn::createGenericColumn('col2'),
            ],
            'primaryKeys' => ['colNotExisting'],
            'exceptionString' => 'Trying to set colNotExisting as PKs but not present in columns',
        ];
        yield 'pk on nullable type' => [
            'cols' => [
                new SnowflakeColumn('col1', new Snowflake(Snowflake::TYPE_VARCHAR, ['nullable' => true])),
            ],
            'primaryKeys' => ['col1'],
            'exceptionString' => 'Trying to set PK on column col1 but this column is nullable',
        ];
    }

    public function testCreateTableWithInvalidTableName(): void
    {
        $this->expectException(QueryBuilderException::class);
        $this->expectExceptionMessage(
            'Invalid table name testTable.: Only alphanumeric characters, underscores and dollar signs are allowed.'
        );
        $this->qb->getCreateTableCommand('testDb', 'testTable.', new ColumnCollection([]));
        self::fail('Should fail because of invalid table name');
    }

    public function testRenameTableWithInvalidTableName(): void
    {
        $this->expectException(QueryBuilderException::class);
        $this->expectExceptionMessage(
            'Invalid table name testTable.: Only alphanumeric characters, underscores and dollar signs are allowed.'
        );
        $this->qb->getRenameTableCommand('testDb', 'testTable', 'testTable.');
        self::fail('Should fail because of invalid table name');
    }

    public function testGetRenameTable(): void
    {
        $renameCommand = $this->qb->getRenameTableCommand('testDb', 'testTable', 'newTable');
        self::assertEquals('ALTER TABLE "testDb"."testTable" RENAME TO "testDb"."newTable"', $renameCommand);
    }

    public function testGetDropTable(): void
    {
        $dropTableCommand = $this->qb->getDropTableCommand('testDb', 'testTable');
        self::assertEquals('DROP TABLE "testDb"."testTable"', $dropTableCommand);
    }

    public function testGetTruncateTable(): void
    {
        $dropTableCommand = $this->qb->getTruncateTableCommand('testDb', 'testTable');
        self::assertEquals('TRUNCATE TABLE "testDb"."testTable"', $dropTableCommand);
    }
}
