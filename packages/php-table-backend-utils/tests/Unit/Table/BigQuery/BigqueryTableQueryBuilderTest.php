<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Table\BigQuery;

use Generator;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Datatype\Definition\Common;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\QueryBuilderException;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Tests\Keboola\TableBackendUtils\Functional\Bigquery\BigqueryBaseCase;
use Throwable;

class BigqueryTableQueryBuilderTest extends BigqueryBaseCase
{
    private BigqueryTableQueryBuilder $qb;

    public function setUp(): void
    {
        $this->qb = new BigqueryTableQueryBuilder();
    }

    /**
     * @param string[] $expectedCommands
     * @param string[] $keys
     * @dataProvider alterColumnCommandProvider
     */
    public function testAlterColumnCommandOnValidCases(
        Bigquery $newDefinition,
        array $expectedCommands,
        array $keys,
    ): void {
        $commands = $this->qb->getUpdateColumnFromDefinitionQuery(
            $newDefinition,
            'mydataset',
            'mytable',
            'mycolumn',
            $keys,
        );
        $this->assertEquals($expectedCommands, $commands);

        // not asked
        $commands = $this->qb->getUpdateColumnFromDefinitionQuery($newDefinition, 'mydataset', 'mytable', 'mycolumn');
        $this->assertEquals([], $commands);
    }

    /**
     * @dataProvider alterColumnCommandInvalidProvider
     */
    public function testAlterColumnCommandInvalid(
        Bigquery $newDefinition,
        QueryBuilderException $expectedException,
    ): void {
        try {
            $this->qb->getUpdateColumnFromDefinitionQuery(
                $newDefinition,
                'mydataset',
                'mytable',
                'mycolumn',
                Common::KBC_METADATA_KEYS_FOR_COLUMNS_SYNC,
            );
            $this->fail('Expected exception not thrown');
        } catch (QueryBuilderException $e) {
            $this->assertEquals($expectedException->getMessage(), $e->getMessage());
            $this->assertEquals($expectedException->getStringCode(), $e->getStringCode());
        }
    }

    public function alterColumnCommandProvider(): Generator
    {
        yield 'REQUIRED -> NULLABLE' => [
            new Bigquery(Bigquery::TYPE_STRING, ['nullable' => true]),
            [
                Common::KBC_METADATA_KEY_NULLABLE
                => 'ALTER TABLE `mydataset`.`mytable` ALTER COLUMN `mycolumn` DROP NOT NULL;',
            ],
            [Common::KBC_METADATA_KEY_NULLABLE],
        ];

        yield 'STRING 65 -> STRING 64' => [
            new Bigquery(Bigquery::TYPE_STRING, ['length' => '64']),
            [
                Common::KBC_METADATA_KEY_LENGTH
                => 'ALTER TABLE `mydataset`.`mytable` ALTER COLUMN `mycolumn` SET DATA TYPE STRING(64);',
            ],
            [Common::KBC_METADATA_KEY_LENGTH],
        ];

        yield 'DEFAULT STRING abc -> xyz' => [
            new Bigquery(Bigquery::TYPE_STRING, ['default' => 'xyz']),
            [
                Common::KBC_METADATA_KEY_DEFAULT
                => "ALTER TABLE `mydataset`.`mytable` ALTER COLUMN `mycolumn` SET DEFAULT 'xyz';",
            ],
            [Common::KBC_METADATA_KEY_DEFAULT],
        ];
    }

    public function alterColumnCommandInvalidProvider(): Generator
    {
        yield 'Setting default "sdfsadf" on BOOLEAN type' => [
            new Bigquery(Bigquery::TYPE_BOOL, ['default' => 'abc']),
            new QueryBuilderException(
                'Invalid default value for column "mycolumn". Allowed values are true, false, 0, 1, got "abc".',
                'invalidDefaultValueForBooleanColumn',
            ),
        ];
    }

    public function testCreateTableWithPrimaryKey(): void
    {
        $columns = new ColumnCollection([
            new BigqueryColumn(
                'id',
                new Bigquery(Bigquery::TYPE_INTEGER),
            ),
            new BigqueryColumn(
                'name',
                new Bigquery(Bigquery::TYPE_STRING),
            ),
        ]);

        $sql = $this->qb->getCreateTableCommand(
            'mydataset',
            'mytable',
            $columns,
            ['id'],
        );

        $expectedSql = <<<SQL
CREATE TABLE `mydataset`.`mytable` 
(
`id` INTEGER,
`name` STRING,
PRIMARY KEY (`id`) NOT ENFORCED
);
SQL;

        $this->assertSame($expectedSql, $sql);
    }

    public function testCreateTableWithCompositePrimaryKey(): void
    {
        $columns = new ColumnCollection([
            new BigqueryColumn(
                'id',
                new Bigquery(Bigquery::TYPE_INTEGER),
            ),
            new BigqueryColumn(
                'type',
                new Bigquery(Bigquery::TYPE_STRING),
            ),
            new BigqueryColumn(
                'name',
                new Bigquery(Bigquery::TYPE_STRING),
            ),
        ]);

        $sql = $this->qb->getCreateTableCommand(
            'mydataset',
            'mytable',
            $columns,
            ['id', 'type'],
        );

        $expectedSql = <<<SQL
CREATE TABLE `mydataset`.`mytable` 
(
`id` INTEGER,
`type` STRING,
`name` STRING,
PRIMARY KEY (`id`,`type`) NOT ENFORCED
);
SQL;

        $this->assertSame($expectedSql, $sql);
    }

    public function testCreateTableWithInvalidPrimaryKey(): void
    {
        $columns = new ColumnCollection([
            new BigqueryColumn(
                'id',
                new Bigquery(Bigquery::TYPE_INTEGER),
            ),
            new BigqueryColumn(
                'name',
                new Bigquery(Bigquery::TYPE_STRING),
            ),
        ]);

        $this->expectException(QueryBuilderException::class);
        $this->expectExceptionMessage('Trying to set "nonexistent" as PKs but not present in columns');

        $this->qb->getCreateTableCommand(
            'mydataset',
            'mytable',
            $columns,
            ['nonexistent'],
        );
    }
}
