<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional\Profile\Column;

use Generator;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\StorageDriver\Snowflake\Profile\Column\DuplicateCountColumnMetric;
use Keboola\StorageDriver\Snowflake\Profile\Column\NullCountColumnMetric;
use Keboola\StorageDriver\Snowflake\Profile\ColumnMetricInterface;
use Keboola\StorageDriver\Snowflake\Tests\Functional\BaseCase;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;

final class BooleanColumnMetricTest extends BaseCase
{
    private const TABLE_NAME = 'metric_column_boolean_test';
    private const COLUMN_BOOL_NOT_NULLABLE = 'bool_not_nullable';
    private const COLUMN_BOOL_NULLABLE = 'bool_nullable';
//    private const COLUMN_STRING_NOT_NULLABLE = 'string_not_nullable'; @todo Test string columns equivalent to boolean
//    private const COLUMN_STRING_NULLABLE = 'string_nullable';

    /**
     * @dataProvider metricProvider
     */
    public function testMetric(
        ColumnMetricInterface $metric,
        string $column,
        int $expected,
    ): void {
        $result = $metric->collect(self::SCHEMA_NAME, self::TABLE_NAME, $column, $this->connection);
        $this->assertSame($expected, $result);
    }

    public static function metricProvider(): Generator
    {
        yield 'duplicateCount (bool, not nullable)' => [
            new DuplicateCountColumnMetric(),
            self::COLUMN_BOOL_NOT_NULLABLE,
            4,
        ];

        yield 'duplicateCount (bool, nullable)' => [
            'metric' => new DuplicateCountColumnMetric(),
            'column' => self::COLUMN_BOOL_NULLABLE,
            'expected' => 2,
        ];

        yield 'nullCountNot (bool, nullable)' => [
            'metric' => new NullCountColumnMetric(),
            'column' => self::COLUMN_BOOL_NOT_NULLABLE,
            'expected' => 0,
        ];

        yield 'nullCount (bool, nullable)' => [
            'metric' => new NullCountColumnMetric(),
            'column' => self::COLUMN_BOOL_NULLABLE,
            'expected' => 2,
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
                        self::COLUMN_BOOL_NOT_NULLABLE,
                        new Snowflake(Snowflake::TYPE_BOOLEAN, ['nullable' => false]),
                    ),
                    new SnowflakeColumn(
                        self::COLUMN_BOOL_NULLABLE,
                        new Snowflake(Snowflake::TYPE_BOOLEAN, ['nullable' => true]),
                    ),
                ]),
            ),
        );

        $this->connection->executeQuery(sprintf(
            <<<'SQL'
                INSERT INTO %s.%s (%s, %s) VALUES
                (TRUE, TRUE),
                (TRUE, TRUE),
                (FALSE, FALSE),
                (FALSE, FALSE),
                (TRUE, NULL),
                (FALSE, NULL);
                SQL,
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_BOOL_NOT_NULLABLE),
            SnowflakeQuote::quoteSingleIdentifier(self::COLUMN_BOOL_NULLABLE),
        ));
    }
}
