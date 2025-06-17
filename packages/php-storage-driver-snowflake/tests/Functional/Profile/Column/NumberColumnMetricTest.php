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

final class NumberColumnMetricTest extends BaseCase
{
    private const TABLE_NAME = 'metric_column_number_test';
    private const COLUMN_NUMBER_NOT_NULLABLE = 'number_not_nullable';
    private const COLUMN_NUMBER_NULLABLE = 'number_nullable';
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
                ]),
            ),
        );

        $this->connection->executeQuery(sprintf(
            <<<'SQL'
                INSERT INTO %s.%s (%s, %s, %s, %s) VALUES
                (1, 1, '1', '1'),
                (2, 2, '2', '2'),
                (3, NULL, '3', NULL),
                (3, NULL, '3', NULL),
                (4, 4, '4', '4'),
                (5, 5, '5', '5'),
                (5, 5, '5', '5'),
                (100, 100, '100', '100'),
                (0, 0, '0', '0'),
                (-10, -10, '-10', '-10'),
                (999999, NULL, '999999', NULL);
                SQL,
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_NUMBER_NOT_NULLABLE),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_NUMBER_NULLABLE),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_VARCHAR_NOT_NULLABLE),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_VARCHAR_NULLABLE),
        ));
    }
}
