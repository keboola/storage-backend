<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Bigquery;

use Google\Cloud\BigQuery\Timestamp;

final class Partition
{
    public function __construct(
        public readonly string $partitionId,
        public readonly string $rowsNumber,
        public readonly string $lastModifiedTime,
        public readonly string $storageTier,
    ) {
    }
}
