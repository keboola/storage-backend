<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table;

use Keboola\TableBackendUtils\Column\ColumnCollection;

interface TableReflectionInterface
{
    /**
     * @return string[]
     */
    public function getColumnsNames(): array;

    public function getColumnsDefinitions(): ColumnCollection;

    public function getRowsCount(): int;

    /**
     * @return string[]
     */
    public function getPrimaryKeysNames(): array;

    public function getTableStats(): TableStatsInterface;

    public function isTemporary(): bool;

    /**
     * @return array{
     *  schema_name: string,
     *  name: string
     * }[]
     */
    public function getDependentViews(): array;
}
