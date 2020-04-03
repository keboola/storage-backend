<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table;

use Keboola\TableBackendUtils\Column\ColumnIterator;

interface TableReflectionInterface
{
    /**
     * @return string[]
     */
    public function getColumnsNames(): array;

    public function getColumnsDefinitions(): ColumnIterator;

    public function getRowsCount(): int;

    /**
     * @return string[]
     */
    public function getPrimaryKeysNames(): array;

    public function getTableStats(): TableStatsInterface;

    public function isTemporary(): bool;
}
