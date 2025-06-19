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

final class DateColumnMetricTest extends BaseCase
{
    private const TABLE_NAME = 'metric_column_date_test';
    private const COLUMN_DATE_NOT_NULLABLE = 'date_not_nullable';
    private const COLUMN_DATE_NULLABLE = 'date_nullable';
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
        yield 'distinctCount (date, not nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_DATE_NOT_NULLABLE,
            7,
        ];

        yield 'distinctCount (varchar, not nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_VARCHAR_NOT_NULLABLE,
            7,
        ];

        yield 'distinctCount (date, nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_DATE_NULLABLE,
            5,
        ];

        yield 'distinctCount (varchar, nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_VARCHAR_NULLABLE,
            5,
        ];

        yield 'duplicateCount (date, not nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_DATE_NOT_NULLABLE,
            2,
        ];

        yield 'duplicateCount (varchar, not nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_VARCHAR_NOT_NULLABLE,
            2,
        ];

        yield 'duplicateCount (date, nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_DATE_NULLABLE,
            1,
        ];

        yield 'duplicateCount (varchar, nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_VARCHAR_NULLABLE,
            1,
        ];

        yield 'nullCount (date, not nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_DATE_NOT_NULLABLE,
            0,
        ];

        yield 'nullCount (varchar, not nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_VARCHAR_NOT_NULLABLE,
            0,
        ];

        yield 'nullCount (date, nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_DATE_NULLABLE,
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
                        self::COLUMN_DATE_NOT_NULLABLE,
                        new Snowflake(Snowflake::TYPE_DATE, ['nullable' => false]),
                    ),
                    new SnowflakeColumn(
                        self::COLUMN_DATE_NULLABLE,
                        new Snowflake(Snowflake::TYPE_DATE, ['nullable' => true]),
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
                (DATE '2023-01-01', DATE '2023-01-01', '2023-01-01', '2023-01-01'),
                (DATE '2023-01-02', NULL, '2023-01-02', NULL),
                (DATE '2023-01-02', NULL, '2023-01-02', NULL),
                (DATE '2023-01-03', DATE '2023-01-03', '2023-01-03', '2023-01-03'),
                (DATE '2023-01-03', DATE '2023-01-03', '2023-01-03', '2023-01-03'),
                (DATE '2022-12-31', DATE '2022-12-31', '2022-12-31', '2022-12-31'),
                (DATE '2024-02-29', DATE '2024-02-29', '2024-02-29', '2024-02-29'),
                (DATE '2023-12-25', NULL, '2023-12-25', NULL),
                (DATE '1991-12-02', DATE '1991-12-02', '1991-12-02', '1991-12-02');
                SQL,
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_DATE_NOT_NULLABLE),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_DATE_NULLABLE),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_VARCHAR_NOT_NULLABLE),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_VARCHAR_NULLABLE),
        ));
    }
}
