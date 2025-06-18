<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend;

use Exception as NativeException;
use Generator;
use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\DefinitionInterface;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Backend\Assert;
use Keboola\Db\ImportExport\Exception\ColumnsMismatchException;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\TableType;
use PHPUnit\Framework\TestCase;
use Throwable;

class AssertTest extends TestCase
{
    public function testAssertSameColumns(): void
    {
        $this->expectNotToPerformAssertions();
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                columnName: 'test2',
                columnDefinition: new Snowflake(
                    type: Snowflake::TYPE_TIME,
                    options: [
                        'length' => '3',
                    ],
                ),
            ),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                columnName: 'test2',
                columnDefinition: new Snowflake(
                    type: Snowflake::TYPE_TIME,
                    options: [
                        'length' => '3',
                    ],
                ),
            ),
        ];

        Assert::assertSameColumns(
            source: new ColumnCollection($sourceCols),
            destination: new ColumnCollection($destCols),
        );
    }

    /**
     * @doesNotPerformAssertions
     */
    public function testAssertSameColumnsIgnore(): void
    {
        // first in cols
        Assert::assertSameColumns(
            source: new ColumnCollection([
                SnowflakeColumn::createGenericColumn('test2'),
                SnowflakeColumn::createGenericColumn('test'),
                SnowflakeColumn::createGenericColumn('test1'),
            ]),
            destination: new ColumnCollection([
                SnowflakeColumn::createGenericColumn('test'),
                SnowflakeColumn::createGenericColumn('test1'),
            ]),
            ignoreSourceColumns: ['test2'],
        );

        // middle
        Assert::assertSameColumns(
            source: new ColumnCollection([
                SnowflakeColumn::createGenericColumn('test'),
                SnowflakeColumn::createGenericColumn('test2'),
                SnowflakeColumn::createGenericColumn('test1'),
            ]),
            destination: new ColumnCollection([
                SnowflakeColumn::createGenericColumn('test'),
                SnowflakeColumn::createGenericColumn('test1'),
            ]),
            ignoreSourceColumns: ['test2'],
        );

        // end
        Assert::assertSameColumns(
            source: new ColumnCollection([
                SnowflakeColumn::createGenericColumn('test'),
                SnowflakeColumn::createGenericColumn('test1'),
                SnowflakeColumn::createGenericColumn('test2'),
            ]),
            destination: new ColumnCollection([
                SnowflakeColumn::createGenericColumn('test'),
                SnowflakeColumn::createGenericColumn('test1'),
            ]),
            ignoreSourceColumns: ['test2'],
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
            source: new ColumnCollection($sourceCols),
            destination: new ColumnCollection($destCols),
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
            source: new ColumnCollection($sourceCols),
            destination: new ColumnCollection($destCols),
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
            source: new ColumnCollection($sourceCols),
            destination: new ColumnCollection($destCols),
        );
    }

    public function testAssertSameColumnsInvalidType(): void
    {
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                columnName: 'test2',
                columnDefinition: new Snowflake(
                    type: Snowflake::TYPE_TIME,
                    options: [
                        'length' => '3',
                    ],
                ),
            ),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                columnName: 'test2',
                columnDefinition: new Snowflake(
                    type: Snowflake::TYPE_TIMESTAMP_NTZ,
                    options: [
                        'length' => '3',
                    ],
                ),
            ),
        ];

        $this->expectException(Exception::class);
        $this->expectExceptionMessage(
            'Source destination columns mismatch. "test2 TIME (3)"->"test2 TIMESTAMP_NTZ (3)"',
        );
        Assert::assertSameColumns(
            source: new ColumnCollection($sourceCols),
            destination: new ColumnCollection($destCols),
        );
    }

    public function testAssertSameColumnsInvalidLength(): void
    {
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                columnName: 'test2',
                columnDefinition: new Snowflake(
                    type: Snowflake::TYPE_TIME,
                    options: [
                        'length' => '3',
                    ],
                ),
            ),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                columnName: 'test2',
                columnDefinition: new Snowflake(
                    type: Snowflake::TYPE_TIME,
                    options: [
                        'length' => '4',
                    ],
                ),
            ),
        ];

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Source destination columns mismatch. "test2 TIME (3)"->"test2 TIME (4)"');
        Assert::assertSameColumns(
            source: new ColumnCollection($sourceCols),
            destination: new ColumnCollection($destCols),
        );
    }

    public function testAssertSameColumnsInvalidLength2(): void
    {
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                columnName: 'test2',
                columnDefinition: new Snowflake(Snowflake::TYPE_TIME),
            ),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                columnName: 'test2',
                columnDefinition: new Snowflake(
                    type: Snowflake::TYPE_TIME,
                    options: [
                        'length' => '4',
                    ],
                ),
            ),
        ];

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Source destination columns mismatch. "test2 TIME"->"test2 TIME (4)"');
        Assert::assertSameColumns(
            source: new ColumnCollection($sourceCols),
            destination: new ColumnCollection($destCols),
        );
    }

    public function dataProviderAssertTypesLength(): Generator
    {
        yield 'simple length equals' => [
            'sourceType' => 'simpleType',
            'sourceLength' => '1',
            'destType' => 'simpleType',
            'destLength' => '1',
        ];
        yield 'simple length higher' => [
            'sourceType' => 'simpleType',
            'sourceLength' => '1',
            'destType' => 'simpleType',
            'destLength' => '2',
        ];
        yield 'simple length lower' => [
            'sourceType' => 'simpleType',
            'sourceLength' => '2',
            'destType' => 'simpleType',
            'destLength' => '1',
            'expectedException' => [
                Exception::class,
                'Source destination columns mismatch. "test2 simpleType(2)"->"test2 simpleType(1)"',
            ],
        ];
        yield 'complex length "simple" equals' => [
            'sourceType' => 'complexType',
            'sourceLength' => '1',
            'destType' => 'complexType',
            'destLength' => '1',
        ];
        yield 'complex length "simple" higher' => [
            'sourceType' => 'complexType',
            'sourceLength' => '1',
            'destType' => 'complexType',
            'destLength' => '2',
        ];
        yield 'complex length "simple" lower' => [
            'sourceType' => 'complexType',
            'sourceLength' => '2',
            'destType' => 'complexType',
            'destLength' => '1',
            'expectedException' => [
                Exception::class,
                'Source destination columns mismatch. "test2 complexType(2)"->"test2 complexType(1)"',
            ],
        ];
        yield 'complex length "complex" equals' => [
            'sourceType' => 'complexType',
            'sourceLength' => '2,2',
            'destType' => 'complexType',
            'destLength' => '2,2',
        ];
        yield 'complex length "complex" both higher' => [
            'sourceType' => 'complexType',
            'sourceLength' => '2,2',
            'destType' => 'complexType',
            'destLength' => '3,3',
        ];
        yield 'complex length "complex" precision higher' => [
            'sourceType' => 'complexType',
            'sourceLength' => '2,2',
            'destType' => 'complexType',
            'destLength' => '3,2',
        ];
        yield 'complex length "complex" scale higher' => [
            'sourceType' => 'complexType',
            'sourceLength' => '2,2',
            'destType' => 'complexType',
            'destLength' => '2,3',
        ];
        yield 'complex length "complex" both lower' => [
            'sourceType' => 'complexType',
            'sourceLength' => '2,2',
            'destType' => 'complexType',
            'destLength' => '1,1',
            'expectedException' => [
                Exception::class,
                'Source destination columns mismatch. "test2 complexType(2,2)"->"test2 complexType(1,1)"',
            ],
        ];
        yield 'complex length "complex" precision lower' => [
            'sourceType' => 'complexType',
            'sourceLength' => '2,2',
            'destType' => 'complexType',
            'destLength' => '1,2',
            'expectedException' => [
                Exception::class,
                'Source destination columns mismatch. "test2 complexType(2,2)"->"test2 complexType(1,2)"',
            ],
        ];
        yield 'complex length "complex" scale lower' => [
            'sourceType' => 'complexType',
            'sourceLength' => '2,2',
            'destType' => 'complexType',
            'destLength' => '2,1',
            'expectedException' => [
                Exception::class,
                'Source destination columns mismatch. "test2 complexType(2,2)"->"test2 complexType(2,1)"',
            ],
        ];
        yield 'other type complex length equals' => [
            'sourceType' => 'otherType',
            'sourceLength' => '2,2xx',
            'destType' => 'otherType',
            'destLength' => '2,2xx',
        ];
        yield 'other type length lower' => [
            'sourceType' => 'otherType',
            'sourceLength' => '2',
            'destType' => 'otherType',
            'destLength' => '1',
            'expectedException' => [
                Exception::class,
                'Source destination columns mismatch. "test2 otherType(2)"->"test2 otherType(1)"',
            ],
        ];
        yield 'other type length higher' => [
            'sourceType' => 'otherType',
            'sourceLength' => '2',
            'destType' => 'otherType',
            'destLength' => '3',
            'expectedException' => [
                Exception::class,
                'Source destination columns mismatch. "test2 otherType(2)"->"test2 otherType(3)"',
            ],
        ];
    }

    /**
     * @dataProvider dataProviderAssertTypesLength
     * @param array{class-string<Throwable>, string}|null $expectedException
     */
    public function testAssertSameColumnsWithSpecificLengths(
        string $sourceType,
        string $sourceLength,
        string $destType,
        string $destLength,
        array|null $expectedException = null,
    ): void {
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            $this->getColumn(type: $sourceType, length: $sourceLength),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            $this->getColumn(type: $destType, length: $destLength),
        ];

        if ($expectedException !== null) {
            [$exceptionClass, $exceptionMessage] = $expectedException;
            $this->expectException($exceptionClass);
            $this->expectExceptionMessage($exceptionMessage);
        } else {
            $this->expectNotToPerformAssertions();
        }
        Assert::assertSameColumns(
            source: new ColumnCollection($sourceCols),
            destination: new ColumnCollection($destCols),
            complexLengthTypes: ['COMPLEXTYPE'],
            simpleLengthTypes: ['SIMPLETYPE'],
        );
    }

    private function getColumn(string $type, string $length): ColumnInterface
    {
        return new class($type, $length) implements ColumnInterface {
            public function __construct(private readonly string $type, private readonly string $length)
            {
            }

            public function getColumnName(): string
            {
                return 'test2';
            }

            public function getColumnDefinition(): DefinitionInterface
            {
                return new class($this->type, ['length' => $this->length]) extends Common {
                    public function getSQLDefinition(): string
                    {
                        return sprintf('%s(%s)', $this->getType(), $this->getLength());
                    }

                    public function getBasetype(): string
                    {
                        throw new NativeException('Not implemented');
                    }

                    public function toArray(): array
                    {
                        throw new NativeException('Not implemented');
                    }

                    public static function getTypeByBasetype(string $basetype): string
                    {
                        throw new NativeException('Not implemented');
                    }

                    public static function getDefinitionForBasetype(string $basetype): DefinitionInterface
                    {
                        throw new NativeException('Not implemented');
                    }
                };
            }

            public static function createGenericColumn(string $columnName): ColumnInterface
            {
                throw new NativeException('Not implemented');
            }

            public static function createTimestampColumn(
                string $columnName = self::TIMESTAMP_COLUMN_NAME,
            ): ColumnInterface {
                throw new NativeException('Not implemented');
            }

            /**
             * @param array<mixed> $dbResponse
             */
            public static function createFromDB(array $dbResponse): ColumnInterface
            {
                throw new NativeException('Not implemented');
            }
        };
    }

    /**
     * @doesNotPerformAssertions
     */
    public function testAssertSameColumnsInvalidLengthIgnore(): void
    {
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                columnName: 'test2',
                columnDefinition: new Snowflake(Snowflake::TYPE_TIME),
            ),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                columnName: 'test2',
                columnDefinition: new Snowflake(
                    type: Snowflake::TYPE_TIME,
                    options: [
                        'length' => '4',
                    ],
                ),
            ),
        ];

        Assert::assertSameColumns(
            source: new ColumnCollection($sourceCols),
            destination: new ColumnCollection($destCols),
            assertOptions: Assert::ASSERT_MINIMAL,
        );
    }

    public function testAssertSameColumnsStrictLengthPasses(): void
    {
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            new SnowflakeColumn(
                columnName: 'test1',
                columnDefinition: new Snowflake(
                    type: Snowflake::TYPE_VARCHAR,
                    options: [
                        'length' => '10',
                    ],
                ),
            ),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            new SnowflakeColumn(
                columnName: 'test1',
                columnDefinition: new Snowflake(
                    type: Snowflake::TYPE_VARCHAR,
                    options: [
                        'length' => '10',
                    ],
                ),
            ),
        ];
        $this->expectNotToPerformAssertions();
        Assert::assertSameColumns(
            source: new ColumnCollection($sourceCols),
            destination: new ColumnCollection($destCols),
            assertOptions: Assert::ASSERT_STRICT_LENGTH,
        );
    }

    public function testAssertSameColumnsStrictLengthFails(): void
    {
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            new SnowflakeColumn(
                columnName: 'test1',
                columnDefinition: new Snowflake(
                    type: Snowflake::TYPE_VARCHAR,
                    options: [
                        'length' => '10',
                    ],
                ),
            ),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            new SnowflakeColumn(
                columnName: 'test1',
                columnDefinition: new Snowflake(
                    type: Snowflake::TYPE_VARCHAR,
                    options: [
                        'length' => '20',
                    ],
                ),
            ),
        ];
        $this->expectException(Exception::class);
        $this->expectExceptionMessage(
            'Source destination columns mismatch. "test1 VARCHAR (10)"->"test1 VARCHAR (20)"',
        );
        Assert::assertSameColumns(
            source: new ColumnCollection($sourceCols),
            destination: new ColumnCollection($destCols),
            assertOptions: Assert::ASSERT_STRICT_LENGTH,
        );
    }

    public function testAssertPrimaryKeysPasses(): void
    {
        $tableDef1 = $this->createTableDefinitionWithPrimaryKeys(['id', 'name']);
        $tableDef2 = $this->createTableDefinitionWithPrimaryKeys(['name', 'id']); // order should not matter
        $this->expectNotToPerformAssertions();
        Assert::assertPrimaryKeys(source: $tableDef1, destination: $tableDef2);
    }

    public function testAssertPrimaryKeysFails(): void
    {
        $tableDef1 = $this->createTableDefinitionWithPrimaryKeys(['id', 'name']);
        $tableDef2 = $this->createTableDefinitionWithPrimaryKeys(['id', 'other']);
        $this->expectException(ColumnsMismatchException::class);
        Assert::assertPrimaryKeys(source: $tableDef1, destination: $tableDef2);
    }

    /**
     * @param string[] $primaryKeys
     */
    private function createTableDefinitionWithPrimaryKeys(array $primaryKeys): TableDefinitionInterface
    {
        return new class($primaryKeys) implements TableDefinitionInterface {
            /**
             * @param string[] $primaryKeys
             */
            public function __construct(private readonly array $primaryKeys)
            {
            }

            public function getPrimaryKeysNames(): array
            {
                return $this->primaryKeys;
            }

            // ...other methods not needed for this test...
            public function getColumnsDefinitions(): ColumnCollection
            {
                return new ColumnCollection([]);
            }

            public function getTableName(): string
            {
                return 'dummy';
            }

            public function getColumnsNames(): array
            {
                return [];
            }

            public function isTemporary(): bool
            {
                return false;
            }

            public function getTableType(): TableType
            {
                return TableType::TABLE;
            }
        };
    }
}
