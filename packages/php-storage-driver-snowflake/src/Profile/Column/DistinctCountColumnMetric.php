<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Profile\Column;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Snowflake\Profile\ColumnMetricInterface;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

final class DistinctCountColumnMetric implements ColumnMetricInterface
{
    public function name(): string
    {
        return 'distinctCount';
    }

    public function description(): string
    {
        return 'Number of distinct values in the column.';
    }

    public function collect(
        string $schema,
        string $table,
        string $column,
        Connection $connection,
    ): int {
        $sql = sprintf(
            <<<'SQL'
                SELECT COUNT(DISTINCT %s) as distinct_count FROM %s.%s
                SQL,
            SnowflakeQuote::quoteSingleIdentifier($column),
            SnowflakeQuote::quoteSingleIdentifier($schema),
            SnowflakeQuote::quoteSingleIdentifier($table),
        );

        $result = $connection->fetchOne($sql);

        return (int) $result;
    }
}
