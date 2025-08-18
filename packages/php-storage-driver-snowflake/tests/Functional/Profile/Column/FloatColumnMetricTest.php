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

final class FloatColumnMetricTest extends BaseCase
{
    private const TABLE_NAME = 'metric_column_float_test';
    private const COLUMN_FLOAT_NOT_NULLABLE = 'float_not_nullable';
    private const COLUMN_FLOAT_NULLABLE = 'float_nullable';
    private const COLUMN_VARCHAR_NOT_NULLABLE = 'varchar_not_nullable';
    private const COLUMN_VARCHAR_NULLABLE = 'varchar_nullable';

    /**
     * @dataProvider metricProvider
     * @param array<mixed>|int $expected
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
        yield 'distinctCount (float, not nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_FLOAT_NOT_NULLABLE,
            7,
        ];

        yield 'distinctCount (varchar, not nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_VARCHAR_NOT_NULLABLE,
            7,
        ];

        yield 'distinctCount (float, nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_FLOAT_NULLABLE,
            5,
        ];

        yield 'distinctCount (varchar, nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_VARCHAR_NULLABLE,
            5,
        ];

        yield 'duplicateCount (float, not nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_FLOAT_NOT_NULLABLE,
            2,
        ];

        yield 'duplicateCount (varchar, not nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_VARCHAR_NOT_NULLABLE,
            2,
        ];

        yield 'duplicateCount (float, nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_FLOAT_NULLABLE,
            1,
        ];

        yield 'duplicateCount (varchar, nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_VARCHAR_NULLABLE,
            1,
        ];

        yield 'nullCount (float, not nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_FLOAT_NOT_NULLABLE,
            0,
        ];

        yield 'nullCount (varchar, not nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_VARCHAR_NOT_NULLABLE,
            0,
        ];

        yield 'nullCount (float, nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_FLOAT_NULLABLE,
            3,
        ];

        yield 'nullCount (varchar, nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_VARCHAR_NULLABLE,
            3,
        ];

        yield 'numeric statistics (float, not nullable)' => [
            new NumericStatisticsColumnMetric(),
            self::COLUMN_FLOAT_NOT_NULLABLE,
            [
                'avg' => 13830.3376666667,
                'mode' => 4.56,
                'median' => 4.56,
                'min' => -3.21,
                'max' => 123456.789,
            ],
        ];

        // @todo Waiting for implementation of untyped tables profiling.
//        yield 'numeric statistics (varchar, not nullable)' => [
//            new NumericStatisticsColumnMetric(),
//            self::COLUMN_VARCHAR_NOT_NULLABLE,
//            [],
//        ];

        yield 'numeric statistics (float, nullable)' => [
            new NumericStatisticsColumnMetric(),
            self::COLUMN_FLOAT_NULLABLE,
            [
                'avg' => 20577.3215,
                'mode' => 1.23,
                'median' => 1.23,
                'min' => -3.21,
                'max' => 123456.789,
            ],
        ];

        // @todo Waiting for implementation of untyped tables profiling.
//        yield 'numeric statistics (varchar, nullable)' => [
//            new NumericStatisticsColumnMetric(),
//            self::COLUMN_VARCHAR_NULLABLE,
//            [],
//        ];
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
                        self::COLUMN_FLOAT_NOT_NULLABLE,
                        new Snowflake(Snowflake::TYPE_FLOAT, ['nullable' => false]),
                    ),
                    new SnowflakeColumn(
                        self::COLUMN_FLOAT_NULLABLE,
                        new Snowflake(Snowflake::TYPE_FLOAT, ['nullable' => true]),
                    ),
                    new SnowflakeColumn(
                        self::COLUMN_VARCHAR_NOT_NULLABLE,
                        new Snowflake(Snowflake::TYPE_VARCHAR, ['nullable' => false]),
                    ),
                    new SnowflakeColumn(
                        self::COLUMN_VARCHAR_NULLABLE,
                        new Snowflake(Snowflake::TYPE_VARCHAR, ['nullable' => true]),
                    ),
                ]),
            ),
        );

        // @todo Missing test data for -inf, +inf and NaN values.
        $this->connection->executeQuery(sprintf(
            <<<'SQL'
                INSERT INTO %s.%s (%s, %s, %s, %s) VALUES
                (1.23, 1.23, '1.23', '1.23'),
                (1.23, 1.23, '1.23', '1.23'),
                (4.56, NULL, '4.56', NULL),
                (4.56, NULL, '4.56', NULL),
                (7.89, 7.89, '7.89', '7.89'),
                (0.0, 0.0, '0.0', '0.0'),
                (-3.21, -3.21, '-3.21', '-3.21'),
                (123456.789, 123456.789, '123456.789', '123456.789'),
                (999.99, NULL, '999.99', NULL);
                SQL,
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_FLOAT_NOT_NULLABLE),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_FLOAT_NULLABLE),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_VARCHAR_NOT_NULLABLE),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_VARCHAR_NULLABLE),
        ));
    }
}
