<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Profile;

use Doctrine\DBAL\Connection;

interface TableMetricInterface
{
    public function name(): string;

    public function description(): string;

    /**
     * @return array<mixed>
     * @throws MetricCollectFailedException
     */
    public function collect(
        string $schema,
        string $table,
        Connection $connection,
    ): array|bool|float|int|string|null;
}
