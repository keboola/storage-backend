<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Bigquery;

final class ClusteringConfig
{
    /**
     * @param string[] $columns
     */
    public function __construct(
        public readonly array $columns,
    ) {
    }
}
