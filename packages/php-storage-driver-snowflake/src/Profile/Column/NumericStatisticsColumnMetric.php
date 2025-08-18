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
        return 'Basic statistics for numeric column (average, mode, median minimum and maximum).';
    }

    /**
     * @return array{
     *     avg: float|null,
     *     mode: float|null,
     *     median: float|null,
     *     min: float|null,
     *     max: float|null,
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
                SQL,
            $columnQuoted,
            $columnQuoted,
            $columnQuoted,
            $columnQuoted,
            $columnQuoted,
            SnowflakeQuote::quoteSingleIdentifier($schema),
            SnowflakeQuote::quoteSingleIdentifier($table),
        );

        /**
         * @var array{
         *     STATS_AVG: string|null,
         *     STATS_MODE: string|null,
         *     STATS_MEDIAN: string|null,
         *     STATS_MIN: string|null,
         *     STATS_MAX: string|null,
         * } $result
         */
        $result = $connection->fetchAssociative($sql);

        return [
            'avg' => $result['STATS_AVG'] !== null ? (float) $result['STATS_AVG'] : null,
            'mode' => $result['STATS_MODE'] !== null ? (float) $result['STATS_MODE'] : null,
            'median' => $result['STATS_MEDIAN'] !== null ? (float) $result['STATS_MEDIAN'] : null,
            'min' => $result['STATS_MIN'] !== null ? (float) $result['STATS_MIN'] : null,
            'max' => $result['STATS_MAX'] !== null ? (float) $result['STATS_MAX'] : null,
        ];
    }
}
