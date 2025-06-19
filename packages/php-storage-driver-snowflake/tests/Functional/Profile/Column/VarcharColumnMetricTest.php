<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional\Profile\Column;

use Generator;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\StorageDriver\Snowflake\Profile\Column\AvgMinMaxLengthColumnMetric;
use Keboola\StorageDriver\Snowflake\Profile\Column\DistinctCountColumnMetric;
use Keboola\StorageDriver\Snowflake\Profile\Column\DuplicateCountColumnMetric;
use Keboola\StorageDriver\Snowflake\Profile\Column\NullCountColumnMetric;
use Keboola\StorageDriver\Snowflake\Profile\ColumnMetricInterface;
use Keboola\StorageDriver\Snowflake\Tests\Functional\BaseCase;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;

final class VarcharColumnMetricTest extends BaseCase
{
    private const TABLE_NAME = 'metric_column_varchar_test';
    private const COLUMN_NOT_NULLABLE = 'varchar_not_nullable';
    private const COLUMN_NULLABLE = 'varchar_nullable';

    /**
     * @dataProvider metricProvider
     * @param array{avg: float, min: int, max: int}|int $expected
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
        yield 'distinctCount (not nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_NOT_NULLABLE,
            21,
        ];

        yield 'distinctCount (nullable)' => [
            new DistinctCountColumnMetric(),
            self::COLUMN_NULLABLE,
            14,
        ];

        yield 'duplicateCount (not nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_NOT_NULLABLE,
            5,
        ];

        yield 'duplicateCount (nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_NULLABLE,
            4,
        ];

        yield 'nullCount (not nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_NOT_NULLABLE,
            0,
        ];

        yield 'nullCount (nullable)' => [
            new NullCountColumnMetric(),
            self::COLUMN_NULLABLE,
            8,
        ];

        yield 'avgMinMaxLength (not nullable)' => [
            new AvgMinMaxLengthColumnMetric(),
            self::COLUMN_NOT_NULLABLE,
            [
                'avg' => 8.3077,
                'min' => 0,
                'max' => 29,
            ],
        ];

        yield 'avgMinMaxLength (nullable)' => [
            new AvgMinMaxLengthColumnMetric(),
            self::COLUMN_NULLABLE,
            [
                'avg' => 8.2222,
                'min' => 0,
                'max' => 34,
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
                        self::COLUMN_NOT_NULLABLE,
                        new Snowflake(Snowflake::TYPE_VARCHAR, ['nullable' => false]),
                    ),
                    new SnowflakeColumn(
                        self::COLUMN_NULLABLE,
                        new Snowflake(Snowflake::TYPE_VARCHAR, ['nullable' => true]),
                    ),
                ]),
            ),
        );

        $this->connection->executeQuery(sprintf(
            <<<'SQL'
                INSERT INTO %s.%s (%s, %s) VALUES
                ('alpha', 'alpha'),
                ('beta', 'beta'),
                ('gamma', 'gamma'),
                ('delta', 'delta'),
                ('delta', NULL),
                ('delta', NULL),
                ('', ''),
                ('', NULL),
                ('a very long string value here', 'a very very long string value here'),
                ('český', 'český'),
                ('@special!', '@special!'),
                ('12345', '12345'),
                ('user@example.com', 'user@example.com'),
                ('admin@test.org', 'admin@test.org'),
                ('omega', 'omega'),
                ('omega', 'omega'),
                ('more-data', NULL),
                ('test-value', NULL),
                ('xyz', NULL),
                ('duplicate-test', 'duplicate-test'),
                ('duplicate-test', 'duplicate-test'),
                ('unique-1', 'unique-1'),
                ('unique-2', NULL),
                ('unique-3', ''),
                ('empty-again', ''),
                ('null-and-empty', NULL)
                SQL,
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_NOT_NULLABLE),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_NULLABLE),
        ));
    }
}
