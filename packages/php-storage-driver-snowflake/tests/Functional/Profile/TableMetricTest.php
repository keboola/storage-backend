<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional\Profile;

use Keboola\Datatype\Definition\Snowflake;
use Keboola\StorageDriver\Snowflake\Profile\Table\ColumnCountTableMetric;
use Keboola\StorageDriver\Snowflake\Profile\Table\DataSizeTableMetric;
use Keboola\StorageDriver\Snowflake\Profile\Table\RowCountTableMetric;
use Keboola\StorageDriver\Snowflake\Tests\Functional\BaseCase;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;

final class TableMetricTest extends BaseCase
{
    private const TABLE_NAME = 'metric_table_test';

    public function testColumnCount(): void
    {
        $metric = new ColumnCountTableMetric();
        $count = $metric->collect(self::SCHEMA_NAME, self::TABLE_NAME, $this->connection);

        $this->assertSame(7, $count);
    }

    public function testRowCount(): void
    {
        $metric = new RowCountTableMetric();
        $count = $metric->collect(self::SCHEMA_NAME, self::TABLE_NAME, $this->connection);

        $this->assertSame(8, $count);
    }

    public function testDataSize(): void
    {
        $metric = new DataSizeTableMetric();
        $bytes = $metric->collect(self::SCHEMA_NAME, self::TABLE_NAME, $this->connection);

        $this->assertSame(3584, $bytes);
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
                        'id',
                        new Snowflake(Snowflake::TYPE_NUMBER, ['nullable' => false]),
                    ),
                    new SnowflakeColumn('name', new Snowflake(Snowflake::TYPE_VARCHAR)),
                    new SnowflakeColumn('age', new Snowflake(Snowflake::TYPE_NUMBER)),
                    new SnowflakeColumn('signup_date', new Snowflake(Snowflake::TYPE_DATE)),
                    new SnowflakeColumn('email', new Snowflake(Snowflake::TYPE_VARCHAR)),
                    new SnowflakeColumn('isActive', new Snowflake(Snowflake::TYPE_BOOLEAN)),
                    new SnowflakeColumn('score', new Snowflake(Snowflake::TYPE_FLOAT)),
                ]),
            ),
        );

        $this->connection->executeQuery(sprintf(
            <<<'SQL'
                INSERT INTO %s.%s ("id", "name", "age", "signup_date", "email", "isActive", "score") VALUES
                (1, 'Alice', 30, DATE '2024-01-01', 'alice@example.com', TRUE, 85.5),
                (2, 'Bob',  NULL, DATE '2024-02-15', 'bob@example.com', FALSE, NULL),
                (3, 'Charlie', 25, NULL, 'charlie@example.com', TRUE, 92.0),
                (4, 'Alice', 30, DATE '2024-01-01', 'alice@example.com', TRUE, 85.5),
                (5, NULL, NULL, NULL, NULL, NULL, NULL),
                (6, 'Eve', 27, DATE '2024-03-01', 'eve@example.com', TRUE, 78.3),
                (6, 'Eve', 27, DATE '2024-03-01', 'eve@example.com', TRUE, 78.3),
                (7, 'Bob', NULL, DATE '2024-02-15', 'bob@example.com', FALSE, NULL)
                SQL,
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_NAME),
        ));
    }
}
