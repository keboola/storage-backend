<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional\Profile\Column;

use Generator;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\StorageDriver\Snowflake\Profile\Column\DistinctCountColumnMetric;
use Keboola\StorageDriver\Snowflake\Profile\Column\DuplicateCountColumnMetric;
use Keboola\StorageDriver\Snowflake\Profile\Column\NullCountColumnMetric;
use Keboola\StorageDriver\Snowflake\Profile\Column\NumericStatisticsColumnMetric;
use Keboola\StorageDriver\Snowflake\Profile\ColumnMetricInterface;
use Keboola\StorageDriver\Snowflake\Tests\Functional\BaseCase;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;

final class NumberColumnMetricTest extends BaseCase
{
    private const TABLE_NAME = 'metric_column_number_test';
    private const COLUMN_NUMBER_NOT_NULLABLE = 'number_not_nullable';
    private const COLUMN_NUMBER_NULLABLE = 'number_nullable';
    private const COLUMN_VARCHAR_NOT_NULLABLE = 'varchar_not_nullable';
    private const COLUMN_VARCHAR_NULLABLE = 'varchar_nullable';
    private const COLUMN_ONLY_NULL = 'only_null';

    /**
     * @dataProvider metricProvider
     * @param array<mixed> $expected
     */
    public function testMetric(
        ColumnMetricInterface $metric,
        string $column,
        array|int $expected,
    ): void {
        $actual = $metric->collect(self::SCHEMA_NAME, self::TABLE_NAME, $column, $this->connection);
        $this->assertSame($expected, $actual);
    }

    public function metricProvider(): Generator
    {
        yield 'distinctCount (number, not nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_NUMBER_NOT_NULLABLE,
            9,
        ];

        yield 'distinctCount (varchar, not nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_VARCHAR_NOT_NULLABLE,
            9,
        ];

        yield 'distinctCount (number, nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_NUMBER_NULLABLE,
            7,
        ];

        yield 'distinctCount (varchar, nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_VARCHAR_NULLABLE,
            7,
        ];

        yield 'duplicateCount (number, not nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_NUMBER_NOT_NULLABLE,
            2,
        ];

        yield 'duplicateCount (varchar, not nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_VARCHAR_NOT_NULLABLE,
            2,
        ];

        yield 'duplicateCount (number, nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_NUMBER_NULLABLE,
            1,
        ];

        yield 'duplicateCount (varchar, nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_VARCHAR_NULLABLE,
            1,
        ];

        yield 'nullCount (number, not nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_NUMBER_NOT_NULLABLE,
            0,
        ];

        yield 'nullCount (varchar, not nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_VARCHAR_NOT_NULLABLE,
            0,
        ];

        yield 'nullCount (number, nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_NUMBER_NULLABLE,
            3,
        ];

        yield 'nullCount (varchar, nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_VARCHAR_NULLABLE,
            3,
        ];

        yield 'numeric statistics (number, not nullable)' => [
            new NumericStatisticsColumnMetric(),
            self::COLUMN_NUMBER_NOT_NULLABLE,
            [
                'avg' => 90919.272727,
                'median' => 3.0,
                'min' => -10.0,
                'max' => 999999.0,
            ],
        ];

        // @todo Waiting for implementation of untyped tables profiling.
//        yield 'numeric statistics (varchar, not nullable)' => [
//            new NumericStatisticsColumnMetric(),
//            self::COLUMN_VARCHAR_NOT_NULLABLE,
//            [],
//        ];

        yield 'numeric statistics (number, nullable)' => [
            new NumericStatisticsColumnMetric(),
            self::COLUMN_NUMBER_NULLABLE,
            [
                'avg' => 13.375000,
                'median' => 3.0,
                'min' => -10.0,
                'max' => 100.0,
            ],
        ];

        // @todo Waiting for implementation of untyped tables profiling.
//        yield 'numeric statistics (varchar, nullable)' => [
//            new NumericStatisticsColumnMetric(),
//            self::COLUMN_VARCHAR_NULLABLE,
//            [],
//        ];

        yield 'numeric statistics (number, only null)' => [
            new NumericStatisticsColumnMetric(),
            self::COLUMN_ONLY_NULL,
            [
                'avg' => null,
                'median' => null,
                'min' => null,
                'max' => null,
            ],
        ];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->connection->executeQuery(
            (new SnowflakeTableQueryBuilder())->getCreateTableCommand(
                self::SCHEMA_NAME,
                self::TABLE_NAME,
                new ColumnCollection([
                    new SnowflakeColumn(
                        self::COLUMN_NUMBER_NOT_NULLABLE,
                        new Snowflake(Snowflake::TYPE_NUMBER, ['nullable' => false]),
                    ),
                    new SnowflakeColumn(
                        self::COLUMN_NUMBER_NULLABLE,
                        new Snowflake(Snowflake::TYPE_NUMBER, ['nullable' => true]),
                    ),
                    new SnowflakeColumn(
                        self::COLUMN_VARCHAR_NOT_NULLABLE,
                        new Snowflake(Snowflake::TYPE_VARCHAR, ['nullable' => false]),
                    ),
                    new SnowflakeColumn(
                        self::COLUMN_VARCHAR_NULLABLE,
                        new Snowflake(Snowflake::TYPE_VARCHAR, ['nullable' => true]),
                    ),
                    // AVG, MEDIAN, MIN, MAX returns NULL if all records are NULL.
                    new SnowflakeColumn(
                        self::COLUMN_ONLY_NULL,
                        new Snowflake(Snowflake::TYPE_NUMBER, ['nullable' => true]),
                    ),
                ]),
            ),
        );

        $this->connection->executeQuery(sprintf(
            <<<'SQL'
                INSERT INTO %s.%s (%s, %s, %s, %s, %s) VALUES
                (1, 1, '1', '1', NULL),
                (2, 2, '2', '2', NULL),
                (3, NULL, '3', NULL, NULL),
                (3, NULL, '3', NULL, NULL),
                (4, 4, '4', '4', NULL),
                (5, 5, '5', '5', NULL),
                (5, 5, '5', '5', NULL),
                (100, 100, '100', '100', NULL),
                (0, 0, '0', '0', NULL),
                (-10, -10, '-10', '-10', NULL),
                (999999, NULL, '999999', NULL, NULL);
                SQL,
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_NUMBER_NOT_NULLABLE),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_NUMBER_NULLABLE),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_VARCHAR_NOT_NULLABLE),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_VARCHAR_NULLABLE),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_ONLY_NULL),
        ));
    }
}
