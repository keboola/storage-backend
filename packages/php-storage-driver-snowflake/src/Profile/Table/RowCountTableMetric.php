<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Profile\Table;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Snowflake\Profile\TableMetricInterface;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

final class RowCountTableMetric implements TableMetricInterface
{
    public function name(): string
    {
        return 'rowCount';
    }

    public function description(): string
    {
        return 'Number of rows in the table.';
    }

    public function collect(
        string $schema,
        string $table,
        Connection $connection,
    ): int {
        // @todo Not working, returns always zero.
        $sql = sprintf(
            <<<'SQL'
                SELECT ROW_COUNT
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                SQL,
            SnowflakeQuote::quote($schema),
            SnowflakeQuote::quote($table),
        );

        $result = $connection->fetchOne($sql);

        return (int) $result;
    }
}
