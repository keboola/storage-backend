<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Profile\Column;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Keboola\StorageDriver\Snowflake\Profile\ColumnMetricInterface;
use Keboola\StorageDriver\Snowflake\Profile\MetricCollectFailedException;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

final class AvgMinMaxLengthColumnMetric implements ColumnMetricInterface
{
    public function name(): string
    {
        return 'length';
    }

    public function description(): string
    {
        return 'Average, minimum, and maximum length of strings in the column.';
    }

    /**
     * @return array{
     *     avg: float,
     *     min: int,
     *     max: int,
     * }
     */
    public function collect(
        string $schema,
        string $table,
        string $column,
        Connection $connection,
    ): array {
        $columnQuoted = SnowflakeQuote::quoteSingleIdentifier($column);

        $sql = sprintf(
            <<<'SQL'
                SELECT
                    ROUND(AVG(LENGTH(%s)), 4) AS avg_length,
                    MIN(LENGTH(%s)) AS min_length,
                    MAX(LENGTH(%s)) AS max_length
                FROM %s.%s
                WHERE %s IS NOT NULL
                SQL,
            $columnQuoted,
            $columnQuoted,
            $columnQuoted,
            SnowflakeQuote::quoteSingleIdentifier($schema),
            SnowflakeQuote::quoteSingleIdentifier($table),
            $columnQuoted,
        );

        try {
            /** @var array{AVG_LENGTH: float, MIN_LENGTH: int, MAX_LENGTH: int} $result */
            $result = $connection->fetchAssociative($sql);
        } catch (Exception $e) {
            throw MetricCollectFailedException::fromColumnMetric($schema, $table, $column, $this, $e);
        }

        return [
            'avg' => (float) $result['AVG_LENGTH'],
            'min' => (int) $result['MIN_LENGTH'],
            'max' => (int) $result['MAX_LENGTH'],
        ];
    }
}
