<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Bigquery;

final class RangePartitioningConfig
{
    public function __construct(
        public readonly string $column,
        public readonly string $start,
        public readonly string $end,
        public readonly string $interval,
    ) {
    }
}
