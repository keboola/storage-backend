<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column;

use Keboola\Datatype\Definition\DefinitionInterface;
use Keboola\TableBackendUtils\CollectionItemInterface;

interface ColumnInterface extends CollectionItemInterface
{
    public const TIMESTAMP_COLUMN_NAME = '_timestamp';

    public function getColumnName(): string;

    public function getColumnDefinition(): DefinitionInterface;

    /**
     * Will return generic definition for columns used in keboola environment
     * like: varchar(max)
     */
    public static function createGenericColumn(string $columnName): self;
}
