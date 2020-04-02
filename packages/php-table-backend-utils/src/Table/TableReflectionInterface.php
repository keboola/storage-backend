<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table;

use Keboola\TableBackendUtils\Column\SynapseColumn;

interface TableReflectionInterface
{
    /**
     * @return string[]
     */
    public function getColumnsNames(): array;

    /**
     * @return SynapseColumn[]
     */
    public function getColumnsDefinitions(): array;

    public function getRowsCount(): int;

    /**
     * @return string[]
     */
    public function getPrimaryKeysNames(): array;

    public function getTableStats(): TableStatsInterface;

    public function isTemporary(): bool;
}
