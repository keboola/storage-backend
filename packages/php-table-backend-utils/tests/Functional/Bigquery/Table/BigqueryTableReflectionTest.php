<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Bigquery\Table;

use Generator;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\Bigquery\Parser\RESTtoSQLDatatypeConverter;
use Keboola\TableBackendUtils\Column\Bigquery\Parser\SQLtoRestDatatypeConverter;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Keboola\TableBackendUtils\Table\Bigquery\ClusteringConfig;
use Keboola\TableBackendUtils\Table\Bigquery\PartitioningConfig;
use Keboola\TableBackendUtils\Table\Bigquery\RangePartitioningConfig;
use Keboola\TableBackendUtils\Table\Bigquery\TimePartitioningConfig;
use Keboola\TableBackendUtils\Table\TableStats;
use Keboola\TableBackendUtils\TableNotExistsReflectionException;
use Tests\Keboola\TableBackendUtils\Functional\Bigquery\BigqueryBaseCase;

class BigqueryTableReflectionTest extends BigqueryBaseCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanDataset(self::TEST_SCHEMA);
    }

    public function testGetTableColumnsNames(): void
    {
        $this->initTable();
        $ref = new BigqueryTableReflection($this->bqClient, self::TEST_SCHEMA, self::TABLE_GENERIC);

        self::assertSame([
            'id',
            'first_name',
            'last_name',
        ], $ref->getColumnsNames());
    }

    /**
     * @dataProvider tableColsDataProvider
     */
    public function testColumnDefinition(
        string $sqlDef,
        string $expectedSqlDefinition,
        string $expectedType,
        ?string $expectedDefault,
        ?string $expectedLength,
        bool $expectedNullable
    ): void {
        $this->cleanDataset(self::TEST_SCHEMA);
        $this->createDataset(self::TEST_SCHEMA);
        $sql = sprintf(
            '
            CREATE OR REPLACE TABLE %s.%s (
      `firstColumn` INT,
      `column` %s
);',
            BigqueryQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
            BigqueryQuote::quoteSingleIdentifier(self::TABLE_GENERIC),
            $sqlDef
        );

        $query = $this->bqClient->query($sql);
        $this->bqClient->runQuery($query);
        $ref = new BigqueryTableReflection($this->bqClient, self::TEST_SCHEMA, self::TABLE_GENERIC);
        /** @var Generator<BigqueryColumn> $iterator */
        $iterator = $ref->getColumnsDefinitions()->getIterator();
        $iterator->next();
        $column = $iterator->current();
        /** @var Bigquery $definition */
        $definition = $column->getColumnDefinition();
        self::assertEquals($expectedLength, $definition->getLength(), 'length doesnt match');
        self::assertEquals($expectedDefault, $definition->getDefault(), 'default value doesnt match');
        self::assertEquals($expectedType, $definition->getType(), 'type doesnt match');
        self::assertEquals($expectedNullable, $definition->isNullable(), 'nullable flag doesnt match');
        self::assertEquals($expectedSqlDefinition, $definition->getSQLDefinition(), 'SQL definition doesnt match');

        // check that SQLtoRestDatatypeConverter returns same definition as BQ REST API
        self::assertEquals(
            RESTtoSQLDatatypeConverter::convertColumnToSQLFormat(
                $this->bqClient
                    ->dataset(self::TEST_SCHEMA)
                    ->table(self::TABLE_GENERIC)
                    ->info()['schema']['fields'][1]
            ),
            RESTtoSQLDatatypeConverter::convertColumnToSQLFormat(
                SQLtoRestDatatypeConverter::convertColumnToRestFormat($column)
            )
        );
    }

    /**
     * @return Generator<string,array<mixed>>
     */
    public function tableColsDataProvider(): Generator
    {
        yield 'ARRAY simple' => [
            'ARRAY<INT64>',
            // sql which goes to table
            'ARRAY<INTEGER>',
            // expected sql from getSQLDefinition
            'ARRAY', // expected type from db
            null, // default
            'INTEGER', // length
            true, // nullable
        ];
        yield 'ARRAY complex' => [
            'ARRAY<STRUCT<x ARRAY<STRUCT<x_z ARRAY<INT64>,x_v INT64>>,t STRUCT<h INT64>, m NUMERIC(12)>>',
            // sql which goes to table
            'ARRAY<STRUCT<x ARRAY<STRUCT<x_z ARRAY<INTEGER>,x_v INTEGER>>,t STRUCT<h INTEGER>,m NUMERIC(12)>>',
            // expected sql from getSQLDefinition
            'ARRAY', // expected type from db
            null, // default
            'STRUCT<x ARRAY<STRUCT<x_z ARRAY<INTEGER>,x_v INTEGER>>,t STRUCT<h INTEGER>,m NUMERIC(12)>', // length
            true, // nullable
        ];
        yield 'STRUCT simple' => [
            'STRUCT<x INTEGER>',
            // sql which goes to table
            'STRUCT<x INTEGER>',
            // expected sql from getSQLDefinition
            'STRUCT', // expected type from db
            null, // default
            'x INTEGER', // length
            true, // nullable
        ];
        yield 'STRUCT complex' => [
            'STRUCT<x ARRAY<STRUCT<x_z ARRAY<INT64>,x_v INT64>>,t STRUCT<h INT64>, m NUMERIC(12)>',
            // sql which goes to table
            'STRUCT<x ARRAY<STRUCT<x_z ARRAY<INTEGER>,x_v INTEGER>>,t STRUCT<h INTEGER>,m NUMERIC(12)>',
            // expected sql from getSQLDefinition
            'STRUCT', // expected type from db
            null, // default
            'x ARRAY<STRUCT<x_z ARRAY<INTEGER>,x_v INTEGER>>,t STRUCT<h INTEGER>,m NUMERIC(12)', // length
            true, // nullable
        ];
        yield 'DECIMAL' => [
            'DECIMAL(29,0)', // sql which goes to table
            'NUMERIC(29)', // expected sql from getSQLDefinition
            'NUMERIC', // expected type from db
            null, // default
            '29', // length
            true, // nullable
        ];
        yield 'DECIMAL NOT NULL' => [
            'DECIMAL(29,0) NOT NULL', // sql which goes to table
            'NUMERIC(29) NOT NULL', // expected sql from getSQLDefinition
            'NUMERIC', // expected type from db
            null, // default
            '29', // length
            false, // nullable
        ];
        yield 'DECIMAL NOT NULL DEFAULT' => [
            'DECIMAL(29,0) DEFAULT 1 NOT NULL', // sql which goes to table
            'NUMERIC(29) DEFAULT 1 NOT NULL', // expected sql from getSQLDefinition
            'NUMERIC', // expected type from db
            '1', // default
            '29', // length
            false, // nullable
        ];
        yield 'NUMERIC' => [
            'NUMERIC(30,2)', // sql which goes to table
            'NUMERIC(30,2)', // expected sql from getSQLDefinition
            'NUMERIC', // expected type from db
            null, // default
            '30,2', // length
            true, // nullable
        ];
        yield 'BIGDECIMAL' => [
            'BIGDECIMAL(29,0)', // sql which goes to table
            'BIGNUMERIC(29)', // expected sql from getSQLDefinition
            'BIGNUMERIC', // expected type from db
            null, // default
            '29', // length
            true, // nullable
        ];
        yield 'BIGNUMERIC' => [
            'BIGNUMERIC(30,2)', // sql which goes to table
            'BIGNUMERIC(30,2)', // expected sql from getSQLDefinition
            'BIGNUMERIC', // expected type from db
            null, // default
            '30,2', // length
            true, // nullable
        ];
        yield 'INT' => [
            'INT', // sql which goes to table
            'INTEGER', // expected sql from getSQLDefinition
            'INTEGER', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'INTEGER' => [
            'INTEGER', // sql which goes to table
            'INTEGER', // expected sql from getSQLDefinition
            'INTEGER', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'BIGINT' => [
            'BIGINT', // sql which goes to table
            'INTEGER', // expected sql from getSQLDefinition
            'INTEGER', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'SMALLINT' => [
            'SMALLINT', // sql which goes to table
            'INTEGER', // expected sql from getSQLDefinition
            'INTEGER', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'TINYINT' => [
            'TINYINT', // sql which goes to table
            'INTEGER', // expected sql from getSQLDefinition
            'INTEGER', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'BYTEINT' => [
            'BYTEINT', // sql which goes to table
            'INTEGER', // expected sql from getSQLDefinition
            'INTEGER', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'FLOAT64' => [
            'FLOAT64', // sql which goes to table
            'FLOAT64', // expected sql from getSQLDefinition
            'FLOAT64', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'VARCHAR' => [
            'STRING', // sql which goes to table
            'STRING', // expected sql from getSQLDefinition
            'STRING', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'STRING' => [
            'STRING(16777216)', // sql which goes to table
            'STRING(16777216)', // expected sql from getSQLDefinition
            'STRING', // expected type from db
            null, // default
            '16777216', // length
            true, // nullable
        ];
        yield 'BOOL' => [
            'BOOL', // sql which goes to table
            'BOOL', // expected sql from getSQLDefinition
            'BOOL', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'DATE' => [
            'DATE', // sql which goes to table
            'DATE', // expected sql from getSQLDefinition
            'DATE', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'DATETIME' => [
            'DATETIME', // sql which goes to table
            'DATETIME', // expected sql from getSQLDefinition
            'DATETIME', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'TIME' => [
            'TIME', // sql which goes to table
            'TIME', // expected sql from getSQLDefinition
            'TIME', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'TIMESTAMP' => [
            'TIMESTAMP', // sql which goes to table
            'TIMESTAMP', // expected sql from getSQLDefinition
            'TIMESTAMP', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'BYTES' => [
            'BYTES(8388608)', // sql which goes to table
            'BYTES(8388608)', // expected sql from getSQLDefinition
            'BYTES', // expected type from db
            null, // default
            '8388608', // length
            true, // nullable
        ];
        yield 'GEOGRAPHY' => [
            'GEOGRAPHY', // sql which goes to table
            'GEOGRAPHY', // expected sql from getSQLDefinition
            'GEOGRAPHY', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'INTERVAL' => [
            'INTERVAL', // sql which goes to table
            'INTERVAL', // expected sql from getSQLDefinition
            'INTERVAL', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'JSON' => [
            'JSON', // sql which goes to table
            'JSON', // expected sql from getSQLDefinition
            'JSON', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
    }

    public function testGetTableStats(): void
    {
        $this->initTable();
        $ref = new BigqueryTableReflection($this->bqClient, self::TEST_SCHEMA, self::TABLE_GENERIC);

        $stats1 = $ref->getTableStats();
        self::assertEquals(0, $stats1->getRowsCount());
        self::assertEquals(0, $stats1->getDataSizeBytes()); // empty tables take up some space

        $this->insertRowToTable(self::TEST_SCHEMA, self::TABLE_GENERIC, 1, 'lojza', 'lopata');
        $this->insertRowToTable(self::TEST_SCHEMA, self::TABLE_GENERIC, 2, 'karel', 'motycka');

        $ref = new BigqueryTableReflection($this->bqClient, self::TEST_SCHEMA, self::TABLE_GENERIC);
        /** @var TableStats $stats2 */
        $stats2 = $ref->getTableStats();
        self::assertEquals(2, $stats2->getRowsCount());
        self::assertGreaterThan($stats1->getDataSizeBytes(), $stats2->getDataSizeBytes());
    }

    public function testGetRowsCount(): void
    {
        $this->initTable();
        $ref = new BigqueryTableReflection($this->bqClient, self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertEquals(0, $ref->getRowsCount());
        $data = [
            [1, 'franta', 'omacka'],
            [2, 'pepik', 'knedla'],
        ];
        foreach ($data as $item) {
            $this->insertRowToTable(self::TEST_SCHEMA, self::TABLE_GENERIC, ...$item);
        }
        $ref = new BigqueryTableReflection($this->bqClient, self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertEquals(2, $ref->getRowsCount());
    }

    public function testIfTableExists(): void
    {
        $this->initTable();

        $ref = new BigqueryTableReflection($this->bqClient, self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertTrue($ref->exists());
    }

    public function testIfTableDoesNotExists(): void
    {
        $this->initTable();

        $ref = new BigqueryTableReflection($this->bqClient, self::TEST_SCHEMA, 'notExisting');
        self::assertFalse($ref->exists());

        try {
            $ref->getColumnsNames();
            $this->fail('Should failed!');
        } catch (TableNotExistsReflectionException $e) {
            $this->assertSame('Table "notExisting" not found.', $e->getMessage());
        }

        try {
            $ref->getColumnsDefinitions();
            $this->fail('Should failed!');
        } catch (TableNotExistsReflectionException $e) {
            $this->assertSame('Table "notExisting" not found.', $e->getMessage());
        }

        try {
            $ref->getRowsCount();
            $this->fail('Should failed!');
        } catch (TableNotExistsReflectionException $e) {
            $this->assertSame('Table "notExisting" not found.', $e->getMessage());
        }
    }

    public function testTableWithPartitioning(): void
    {
        $dataset = $this->bqClient->createDataset(self::TEST_SCHEMA);
        $dataset->createTable(
            'test-partitions',
            [
                'schema' => [
                    'fields' => [
                        [
                            'name' => 'id',
                            'type' => 'BIGNUMERIC',
                            'mode' => 'NULLABLE',
                        ],
                        [
                            'name' => 'test',
                            'type' => 'STRING',
                            'mode' => 'NULLABLE',
                            'maxLength' => '10',
                        ],
                    ],
                ],
                'timePartitioning' => [
                    'type' => 'DAY',
                    'expirationMs' => null,
                    'field' => null,
                ],
                'rangePartitioning' => null,
                'clustering' => [
                    'fields' => [
                        'test',
                        'id',
                    ],
                ],
                'requirePartitionFilter' => false,
            ]
        );

        $rows = [];
        foreach (range(1, 100) as $row) {
            $rows[] = sprintf('(%d,\'test\')', $row);
        }

        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            <<<SQL
INSERT %s.%s (`id`,`test`) VALUES %s;
SQL,
            BigqueryQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
            BigqueryQuote::quoteSingleIdentifier('test-partitions'),
            implode(',', $rows)
        )));
        $ref = new BigqueryTableReflection($this->bqClient, self::TEST_SCHEMA, 'test-partitions');
        $this->assertCount(1, $ref->getPartitionsList());
    }

    public function partitioningProvider(): Generator
    {
        yield 'Time simple' => [
            'config' => [
                'timePartitioning' => [
                    'type' => 'DAY',
                ],
            ],
            'expectedPartitioning' => new PartitioningConfig(
                new TimePartitioningConfig(
                    'DAY',
                    null,
                    null
                ),
                null,
                false
            ),
            'expectedClustering' => null,
        ];

        yield 'Time field + expiration + filter' => [
            'config' => [
                'timePartitioning' => [
                    'type' => 'DAY',
                    'field' => 'timestamp',
                    'expirationMs' => '100000',
                ],
                'requirePartitionFilter' => true,
            ],
            'expectedPartitioning' => new PartitioningConfig(
                new TimePartitioningConfig(
                    'DAY',
                    '100000',
                    'timestamp'
                ),
                null,
                true
            ),
            'expectedClustering' => null,
        ];

        yield 'Range' => [
            'config' => [
                'rangePartitioning' => [
                    'field' => 'id',
                    'range' => [
                        'start' => '1',
                        'end' => '100',
                        'interval' => '10',
                    ],
                ],
                'requirePartitionFilter' => true,
            ],
            'expectedPartitioning' => new PartitioningConfig(
                null,
                new RangePartitioningConfig(
                    'id',
                    '1',
                    '100',
                    '10'
                ),
                true
            ),
            'expectedClustering' => null,
        ];

        yield 'Clustering, both partitioning created, only range is created' => [
            'config' => [
                'rangePartitioning' => [
                    'field' => 'id',
                    'range' => [
                        'start' => '1',
                        'end' => '100',
                        'interval' => '10',
                    ],
                ],
                'timePartitioning' => [
                    'type' => 'DAY',
                    'field' => 'timestamp',
                    'expirationMs' => '100000',
                ],
                'clustering' => [
                    'fields' => ['id', 'timestamp'],
                ],
                'requirePartitionFilter' => true,
            ],
            'expectedPartitioning' => new PartitioningConfig(
                null,
                new RangePartitioningConfig(
                    'id',
                    '1',
                    '100',
                    '10'
                ),
                true
            ),
            'expectedClustering' => new ClusteringConfig(['id', 'timestamp']),
        ];
    }

    /**
     * @param array<mixed> $config
     * @dataProvider partitioningProvider
     */
    public function testTableWithPartitioningConfig(
        array $config,
        ?PartitioningConfig $expectedPartitioning,
        ?ClusteringConfig $expectedClustering
    ): void {
        $dataset = $this->bqClient->createDataset(self::TEST_SCHEMA);
        $options = [
            'schema' => [
                'fields' => [
                    [
                        'name' => 'id',
                        'type' => 'INTEGER',
                        'mode' => 'NULLABLE',
                    ],
                    [
                        'name' => 'test',
                        'type' => 'STRING',
                        'mode' => 'NULLABLE',
                        'maxLength' => '10',
                    ],
                    [
                        'name' => 'timestamp',
                        'type' => 'TIMESTAMP',
                        'mode' => 'REQUIRED',
                    ],
                ],
            ],
        ];
        $dataset->createTable(
            'test-partitions',
            array_merge($options, $config)
        );

        $ref = new BigqueryTableReflection($this->bqClient, self::TEST_SCHEMA, 'test-partitions');
        $this->assertEquals($expectedClustering, $ref->getClusteringConfiguration());
        $this->assertEquals($expectedPartitioning, $ref->getPartitioningConfiguration());
    }
}
