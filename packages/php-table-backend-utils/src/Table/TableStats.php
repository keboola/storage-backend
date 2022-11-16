<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table;

final class TableStats implements TableStatsInterface
{
    private int $dataSizeBytes;

    private int $rowsCount;

    public function __construct(int $dataSizeBytes, int $rowsCount)
    {
        $this->dataSizeBytes = $dataSizeBytes;
        $this->rowsCount = $rowsCount;
    }

    public function getDataSizeBytes(): int
    {
        return $this->dataSizeBytes;
    }

    public function getRowsCount(): int
    {
        return $this->rowsCount;
    }
}
