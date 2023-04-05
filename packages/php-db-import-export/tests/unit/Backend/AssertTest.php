<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend;

use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Backend\Assert;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use PHPUnit\Framework\TestCase;

class AssertTest extends TestCase
{
    public function testAssertSameColumns(): void
    {
        $this->expectNotToPerformAssertions();
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                'test2',
                new Snowflake(
                    Snowflake::TYPE_TIME,
                    [
                        'length' => '3',
                    ]
                )
            ),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                'test2',
                new Snowflake(
                    Snowflake::TYPE_TIME,
                    [
                        'length' => '3',
                    ]
                )
            ),
        ];

        Assert::assertSameColumns(
            new ColumnCollection($sourceCols),
            new ColumnCollection($destCols)
        );
    }

    /**
     * @doesNotPerformAssertions
     */
    public function testAssertSameColumnsIgnore(): void
    {
        // first in cols
        Assert::assertSameColumns(
            new ColumnCollection([
                SnowflakeColumn::createGenericColumn('test2'),
                SnowflakeColumn::createGenericColumn('test'),
                SnowflakeColumn::createGenericColumn('test1'),
            ]),
            new ColumnCollection([
                SnowflakeColumn::createGenericColumn('test'),
                SnowflakeColumn::createGenericColumn('test1'),
            ]),
            ['test2']
        );

        // middle
        Assert::assertSameColumns(
            new ColumnCollection([
                SnowflakeColumn::createGenericColumn('test'),
                SnowflakeColumn::createGenericColumn('test2'),
                SnowflakeColumn::createGenericColumn('test1'),
            ]),
            new ColumnCollection([
                SnowflakeColumn::createGenericColumn('test'),
                SnowflakeColumn::createGenericColumn('test1'),
            ]),
            ['test2']
        );

        // end
        Assert::assertSameColumns(
            new ColumnCollection([
                SnowflakeColumn::createGenericColumn('test'),
                SnowflakeColumn::createGenericColumn('test1'),
                SnowflakeColumn::createGenericColumn('test2'),
            ]),
            new ColumnCollection([
                SnowflakeColumn::createGenericColumn('test'),
                SnowflakeColumn::createGenericColumn('test1'),
            ]),
            ['test2']
        );
    }

    public function testAssertSameColumnsInvalidCountExtraSource(): void
    {
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            SnowflakeColumn::createGenericColumn('test2'),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
        ];

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Tables don\'t have same number of columns.');
        Assert::assertSameColumns(
            new ColumnCollection($sourceCols),
            new ColumnCollection($destCols)
        );
    }

    public function testAssertSameColumnsInvalidCountExtraDestination(): void
    {
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            SnowflakeColumn::createGenericColumn('test2'),
        ];

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Tables don\'t have same number of columns.');
        Assert::assertSameColumns(
            new ColumnCollection($sourceCols),
            new ColumnCollection($destCols)
        );
    }

    public function testAssertSameColumnsInvalidColumnName(): void
    {
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1x'),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
        ];

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Source destination columns name mismatch. "test1x"->"test1"');
        Assert::assertSameColumns(
            new ColumnCollection($sourceCols),
            new ColumnCollection($destCols)
        );
    }

    public function testAssertSameColumnsInvalidType(): void
    {
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                'test2',
                new Snowflake(
                    Snowflake::TYPE_TIME,
                    [
                        'length' => '3',
                    ]
                )
            ),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                'test2',
                new Snowflake(
                    Snowflake::TYPE_TIMESTAMP_NTZ,
                    [
                        'length' => '3',
                    ]
                )
            ),
        ];

        $this->expectException(Exception::class);
        $this->expectExceptionMessage(
            'Source destination columns mismatch. "test2 TIME (3)"->"test2 TIMESTAMP_NTZ (3)"'
        );
        Assert::assertSameColumns(
            new ColumnCollection($sourceCols),
            new ColumnCollection($destCols)
        );
    }

    public function testAssertSameColumnsInvalidLength(): void
    {
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                'test2',
                new Snowflake(
                    Snowflake::TYPE_TIME,
                    [
                        'length' => '3',
                    ]
                )
            ),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                'test2',
                new Snowflake(
                    Snowflake::TYPE_TIME,
                    [
                        'length' => '4',
                    ]
                )
            ),
        ];

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Source destination columns mismatch. "test2 TIME (3)"->"test2 TIME (4)"');
        Assert::assertSameColumns(
            new ColumnCollection($sourceCols),
            new ColumnCollection($destCols)
        );
    }


    public function testAssertSameColumnsInvalidLength2(): void
    {
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                'test2',
                new Snowflake(Snowflake::TYPE_TIME)
            ),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                'test2',
                new Snowflake(
                    Snowflake::TYPE_TIME,
                    [
                        'length' => '4',
                    ]
                )
            ),
        ];

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Source destination columns mismatch. "test2 TIME"->"test2 TIME (4)"');
        Assert::assertSameColumns(
            new ColumnCollection($sourceCols),
            new ColumnCollection($destCols)
        );
    }
}
