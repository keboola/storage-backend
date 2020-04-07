<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column;

use Keboola\Datatype\Definition\Synapse;

interface ColumnInterface
{
    public const TIMESTAMP_COLUMN_NAME = '_timestamp';

    public function getColumnName(): string;

    public function getColumnDefinition(): Synapse;

    /**
     * Will return generic definition for columns used in keboola environment
     * like: varchar(max)
     */
    public static function createGenericColumn(string $columnName): self;
}
