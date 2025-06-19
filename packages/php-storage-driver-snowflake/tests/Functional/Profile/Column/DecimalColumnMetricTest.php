<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional\Profile\Column;

use Generator;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\StorageDriver\Snowflake\Profile\Column\DistinctCountColumnMetric;
use Keboola\StorageDriver\Snowflake\Profile\Column\DuplicateCountColumnMetric;
use Keboola\StorageDriver\Snowflake\Profile\Column\NullCountColumnMetric;
use Keboola\StorageDriver\Snowflake\Profile\ColumnMetricInterface;
use Keboola\StorageDriver\Snowflake\Tests\Functional\BaseCase;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;

final class DecimalColumnMetricTest extends BaseCase
{
    private const TABLE_NAME = 'metric_column_decimal_test';
    private const COLUMN_DECIMAL_NOT_NULLABLE = 'decimal_not_nullable';
    private const COLUMN_DECIMAL_NULLABLE = 'decimal_nullable';
    private const COLUMN_VARCHAR_NOT_NULLABLE = 'varchar_not_nullable';
    private const COLUMN_VARCHAR_NULLABLE = 'varchar_nullable';

    /**
     * @dataProvider metricProvider
     */
    public function testMetric(
        ColumnMetricInterface $metric,
        string $column,
        int $expected,
    ): void {
        $actual = $metric->collect(self::SCHEMA_NAME, self::TABLE_NAME, $column, $this->connection);
        $this->assertSame($expected, $actual);
    }

    public function metricProvider(): Generator
    {
        yield 'distinctCount (decimal, not nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_DECIMAL_NOT_NULLABLE,
            7,
        ];

        yield 'distinctCount (varchar, not nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_VARCHAR_NOT_NULLABLE,
            7,
        ];

        yield 'distinctCount (decimal, nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_DECIMAL_NULLABLE,
            5,
        ];

        yield 'distinctCount (varchar, nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_VARCHAR_NULLABLE,
            5,
        ];

        yield 'duplicateCount (decimal, not nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_DECIMAL_NOT_NULLABLE,
            2,
        ];

        yield 'duplicateCount (varchar, not nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_VARCHAR_NOT_NULLABLE,
            2,
        ];

        yield 'duplicateCount (decimal, nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_DECIMAL_NULLABLE,
            1,
        ];

        yield 'duplicateCount (varchar, nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_VARCHAR_NULLABLE,
            1,
        ];

        yield 'nullCount (decimal, not nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_DECIMAL_NOT_NULLABLE,
            0,
        ];

        yield 'nullCount (varchar, not nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_VARCHAR_NOT_NULLABLE,
            0,
        ];

        yield 'nullCount (decimal, nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_DECIMAL_NULLABLE,
            3,
        ];

        yield 'nullCount (varchar, nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_VARCHAR_NULLABLE,
            3,
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
                        self::COLUMN_DECIMAL_NOT_NULLABLE,
                        new Snowflake(
                            Snowflake::TYPE_DECIMAL,
                            [
                                'nullable' => false,
                                'length' => [
                                    'numeric_precision' => 20,
                                    'numeric_scale' => 4,
                                ],
                            ],
                        ),
                    ),
                    new SnowflakeColumn(
                        self::COLUMN_DECIMAL_NULLABLE,
                        new Snowflake(
                            Snowflake::TYPE_DECIMAL,
                            [
                                'nullable' => true,
                                'length' => [
                                    'numeric_precision' => 20,
                                    'numeric_scale' => 4,
                                ],
                            ],
                        ),
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

        $this->connection->executeQuery(sprintf(
            <<<'SQL'
                INSERT INTO %s.%s (%s, %s, %s, %s) VALUES
                (10.5, 10.5, '10.5000', '10.5000'),
                (10.5, 10.5, '10.5000', '10.5000'),
                (20.0, NULL, '20.0000', NULL),
                (20.0, NULL, '20.0000', NULL),
                (-5.25, -5.25, '-5.2500', '-5.2500'),
                (0.00, 0.00, '0.0000', '0.0000'),
                (9999999999.9999, 9999999999.99999, '9999999999.99999', '9999999999.99999'),
                (3.141592, NULL, '3.1416', NULL),
                (1.0, 1.0, '1.0000', '1.0000');
                SQL,
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_DECIMAL_NOT_NULLABLE),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_DECIMAL_NULLABLE),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_VARCHAR_NOT_NULLABLE),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_VARCHAR_NULLABLE),
        ));
    }
}
