<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table;

interface TableStatsInterface
{
    public function getDataSizeBytes(): int;

    public function getRowsCount(): int;
}
