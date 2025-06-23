<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Profile\Column;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Snowflake\Profile\ColumnMetricInterface;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

final class NullCountColumnMetric implements ColumnMetricInterface
{
    public function name(): string
    {
        return 'nullCount';
    }

    public function description(): string
    {
        return 'Number of NULL values in the column.';
    }

    public function collect(
        string $schema,
        string $table,
        string $column,
        Connection $connection,
    ): int {
        $sql = sprintf(
            <<<'SQL'
                SELECT COUNT(*) as null_count FROM %s.%s WHERE %s IS NULL
                SQL,
            SnowflakeQuote::quoteSingleIdentifier($schema),
            SnowflakeQuote::quoteSingleIdentifier($table),
            SnowflakeQuote::quoteSingleIdentifier($column),
        );

        /** @var string $result */
        $result = $connection->fetchOne($sql);

        return (int) $result;
    }
}
