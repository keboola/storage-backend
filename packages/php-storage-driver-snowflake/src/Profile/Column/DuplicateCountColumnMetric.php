<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Profile\Column;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Snowflake\Profile\ColumnMetricInterface;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

final class DuplicateCountColumnMetric implements ColumnMetricInterface
{
    public function name(): string
    {
        return 'duplicateCount';
    }

    public function description(): string
    {
        return 'Number of duplicate values in the column.';
    }

    public function collect(
        string $schema,
        string $table,
        string $column,
        Connection $connection,
    ): int {
        $columnQuoted = SnowflakeQuote::quoteSingleIdentifier($column);

        $sql = sprintf(
            <<<'SQL'
                SELECT COUNT(%s) - COUNT(DISTINCT %s) as duplicate_count FROM %s.%s WHERE %s IS NOT NULL
                SQL,
            $columnQuoted,
            $columnQuoted,
            SnowflakeQuote::quoteSingleIdentifier($schema),
            SnowflakeQuote::quoteSingleIdentifier($table),
            $columnQuoted,
        );

        /** @var string $result */
        $result = $connection->fetchOne($sql);

        return (int) $result;
    }
}
