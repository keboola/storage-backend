<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Bigquery;

final class PartitioningConfig
{
    public function __construct(
        public readonly ?TimePartitioningConfig $timePartitioningConfig,
        public readonly ?RangePartitioningConfig $rangePartitioningConfig,
        public readonly bool $requirePartitionFilter,
    ) {
    }
}
