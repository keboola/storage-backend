<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Bigquery;

final class TimePartitioningConfig
{
    public function __construct(
        public readonly string $type,
        public readonly ?string $expirationMs,
        public readonly ?string $column,
    ) {
    }
}
