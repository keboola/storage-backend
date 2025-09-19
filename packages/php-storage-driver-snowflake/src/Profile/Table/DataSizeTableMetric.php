<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Profile\Table;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Keboola\StorageDriver\Snowflake\Profile\MetricCollectFailedException;
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

        try {
            /**
             * @var array{
             *     bytes: string,
             * } $result
             */
            $result = $connection->fetchAssociative($sql);
        } catch (Exception $e) {
            throw MetricCollectFailedException::fromTableMetric($schema, $table, $this, $e);
        }

        return (int) $result['bytes'];
    }
}
