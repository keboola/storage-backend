<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Profile\Column;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Snowflake\Profile\ColumnMetricInterface;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

final class NumericStatisticsColumnMetric implements ColumnMetricInterface
{
    public function name(): string
    {
        return 'numericStatistics';
    }

    public function description(): string
    {
        return 'Basic statistics for numeric column (average, mode, median minimum, and maximum.';
    }

    /**
     * @return array{
     *     avg: float,
     *     mode: float,
     *     median: float,
     *     min: float,
     *     max: float,
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
                    AVG(%s) AS stats_avg,
                    MODE(%s) AS stats_mode,
                    MEDIAN(%s) AS stats_median,
                    MIN(%s) AS stats_min,
                    MAX(%s) AS stats_max
                FROM %s.%s
                WHERE %s IS NOT NULL
                SQL,
            $columnQuoted,
            $columnQuoted,
            $columnQuoted,
            $columnQuoted,
            $columnQuoted,
            SnowflakeQuote::quoteSingleIdentifier($schema),
            SnowflakeQuote::quoteSingleIdentifier($table),
            $columnQuoted,
        );

        /** @var array{
         *     STATS_AVG: string,
         *     STATS_MODE: string,
         *     STATS_MEDIAN: string,
         *     STATS_MIN: string,
         *     STATS_MAX: string
         * } $result
         */
        $result = $connection->fetchAssociative($sql);

        return [
            'avg' => (float) $result['STATS_AVG'],
            'mode' => (float) $result['STATS_MODE'],
            'median' => (float) $result['STATS_MEDIAN'],
            'min' => (float) $result['STATS_MIN'],
            'max' => (float) $result['STATS_MAX'],
        ];
    }
}
