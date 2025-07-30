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
        // The same SQL as in keboola/connection to present same numbers.
        $sql = sprintf(
            'SHOW TABLES LIKE %s IN SCHEMA %s;',
            SnowflakeQuote::quote($table),
            SnowflakeQuote::quoteSingleIdentifier($schema),
        );

        /**
         * @var array{
         *     bytes: string,
         * } $result
         */
        $result = $connection->fetchAssociative($sql);

        return (int) $result['bytes'];
    }
}
