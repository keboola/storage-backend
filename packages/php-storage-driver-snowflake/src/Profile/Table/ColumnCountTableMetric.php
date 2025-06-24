<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Profile\Table;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Snowflake\Profile\TableMetricInterface;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

final class ColumnCountTableMetric implements TableMetricInterface
{
    public function name(): string
    {
        return 'columnCount';
    }

    public function description(): string
    {
        return 'Number of columns in the table.';
    }

    public function collect(
        string $schema,
        string $table,
        Connection $connection,
    ): int {
        $sql = sprintf(
            <<<'SQL'
                SELECT COUNT(*) AS column_count
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s;
                SQL,
            SnowflakeQuote::quote($schema),
            SnowflakeQuote::quote($table),
        );

        /** @var string $result */
        $result = $connection->fetchOne($sql);

        return (int) $result;
    }
}
