<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Profile\Table;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Snowflake\Profile\TableMetricInterface;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

final class DataSizeTableMetric implements TableMetricInterface
{
    public function name(): string
    {
        return 'dataSize';
    }

    public function description(): string
    {
        return 'Allocated size of the table in bytes.';
    }

    public function collect(
        string $schema,
        string $table,
        Connection $connection,
    ): int {
        $sql = sprintf(
            <<<'SQL'
                SELECT ACTIVE_BYTES
                FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
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
