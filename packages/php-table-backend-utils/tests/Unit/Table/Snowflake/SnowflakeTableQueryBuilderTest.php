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
    }

    public function testCreateTableWithInvalidTableName(): void
    {
        $this->expectException(QueryBuilderException::class);
        $this->expectExceptionMessage(
            'Invalid table name testTab.: Only alphanumeric characters, dash,'
                . ' underscores and dollar signs are allowed.',
        );
        $this->qb->getCreateTableCommand('testDb', 'testTab.', new ColumnCollection([]));
        self::fail('Should fail because of invalid table name');
    }

    public function testRenameTableWithInvalidTableName(): void
    {
        $this->expectException(QueryBuilderException::class);
        $this->expectExceptionMessage(
            'Invalid table name testTab.: Only alphanumeric characters, dash,'
                . ' underscores and dollar signs are allowed.',
        );
        $this->qb->getRenameTableCommand('testDb', 'testTab', 'testTab.');
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

    /**
     * @dataProvider provideGetColumnDefinitionUpdate
     */
    public function testGetColumnDefinitionUpdate(
        Snowflake $existingColumn,
        Snowflake $desiredColumn,
        string $expectedQuery,
    ): void {
        $existingColumnDefinition = $existingColumn;
        $desiredColumnDefinition = $desiredColumn;
        $sql = $this->qb->getUpdateColumnFromDefinitionQuery(
            $existingColumnDefinition,
            $desiredColumnDefinition,
            'testDb',
            'testTable',
            'testColumn',
        );
        self::assertEquals(
            $expectedQuery,
            $sql,
        );
    }

    /**
     * @dataProvider provideInvalidGetColumnDefinitionUpdate
     */
    public function testInvalidGetColumnDefinitionUpdate(
        Snowflake $existingColumn,
        Snowflake $desiredColumn,
        string $expectedExceptionMessage,
    ): void {
        $existingColumnDefinition = $existingColumn;
        $desiredColumnDefinition = $desiredColumn;
        $this->expectExceptionMessage($expectedExceptionMessage);
        $this->qb->getUpdateColumnFromDefinitionQuery(
            $existingColumnDefinition,
            $desiredColumnDefinition,
            'testDb',
            'testTable',
            'testColumn',
        );
    }

    /**
     * @return \Generator<string, array{Snowflake,Snowflake,string}>
     */
    public function provideGetColumnDefinitionUpdate(): Generator
    {
        yield 'drop default' => [
            new Snowflake('NUMERIC', ['length' => '12,8', 'nullable' => true, 'default' => '10']),
            new Snowflake('NUMERIC', ['length' => '12,8', 'nullable' => true, 'default' => null]),
            /** @lang Snowflake */
            'ALTER TABLE "testDb"."testTable" MODIFY COLUMN "testColumn" DROP DEFAULT',
        ];
        yield 'add nullable' => [
            new Snowflake('NUMERIC', ['length' => '12,8', 'nullable' => false, 'default' => '']),
            new Snowflake('NUMERIC', ['length' => '12,8', 'nullable' => true, 'default' => '']),
            /** @lang Snowflake */
            'ALTER TABLE "testDb"."testTable" MODIFY COLUMN "testColumn" DROP NOT NULL',
        ];
        yield 'drop nullable' => [
            new Snowflake('NUMERIC', ['length' => '12,8', 'nullable' => true, 'default' => '']),
            new Snowflake('NUMERIC', ['length' => '12,8', 'nullable' => false, 'default' => '']),
            /** @lang Snowflake */
            'ALTER TABLE "testDb"."testTable" MODIFY COLUMN "testColumn" SET NOT NULL',
        ];
        yield 'increase length of text column' => [
            new Snowflake('VARCHAR', ['length' => '12', 'nullable' => true, 'default' => '']),
            new Snowflake('VARCHAR', ['length' => '38', 'nullable' => true, 'default' => '']),
            /** @lang Snowflake */
            'ALTER TABLE "testDb"."testTable" MODIFY COLUMN "testColumn" SET DATA TYPE VARCHAR(38)',
        ];
        yield 'increase precision of numeric column' => [
            new Snowflake('NUMERIC', ['length' => '12,8', 'nullable' => true, 'default' => '']),
            new Snowflake('NUMERIC', ['length' => '14,8', 'nullable' => true, 'default' => '']),
            /** @lang Snowflake */
            'ALTER TABLE "testDb"."testTable" MODIFY COLUMN "testColumn" SET DATA TYPE NUMERIC(14, 8)',
        ];
    }

    public function provideInvalidGetColumnDefinitionUpdate(): Generator
    {
        yield 'add default' => [
            new Snowflake('VARCHAR', ['length' => '10', 'nullable' => true, 'default' => '']),
            new Snowflake('VARCHAR', ['length' => '10', 'nullable' => true, 'default' => 'Bedight']),
            'Cannot change default value of column "testColumn" from "" to "Bedight"',
        ];
        yield 'change default' => [
            new Snowflake('VARCHAR', ['length' => '10', 'nullable' => true, 'default' => 'Bedight']),
            new Snowflake('VARCHAR', ['length' => '10', 'nullable' => true, 'default' => 'Brabble']),
            'Cannot change default value of column "testColumn" from "Bedight" to "Brabble"',
        ];
        yield 'descrease length of string' => [
            new Snowflake('VARCHAR', ['length' => '10', 'nullable' => true, 'default' => 'Bedight']),
            new Snowflake('VARCHAR', ['length' => '8', 'nullable' => true, 'default' => 'Bedight']),
            'Cannot decrease length of column "testColumn" from "10" to "8"',
        ];
        yield 'descrease precision of number' => [
            new Snowflake('NUMERIC', ['length' => '12,8', 'nullable' => true, 'default' => '']),
            new Snowflake('NUMERIC', ['length' => '10,8', 'nullable' => true, 'default' => '']),
            'Cannot decrease precision of column "testColumn" from "12" to "10"',
        ];
        yield 'change scale of number' => [
            new Snowflake('NUMERIC', ['length' => '12,8', 'nullable' => true, 'default' => '']),
            new Snowflake('NUMERIC', ['length' => '12,10', 'nullable' => true, 'default' => '']),
            'Cannot change scale of a column "testColumn" from "8" to "10"',
        ];
    }
}
