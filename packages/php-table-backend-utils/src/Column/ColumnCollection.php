<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column;

use Keboola\TableBackendUtils\Collection;
use Keboola\TableBackendUtils\ColumnException;

/**
 * @extends Collection<ColumnInterface>
 */
final class ColumnCollection extends Collection
{
    public const MAX_TABLE_COLUMNS = 1024;
    /**
     * @param ColumnInterface[] $columns
     */
    public function __construct(array $columns)
    {
        $this->assertTableColumnsCount($columns);
        parent::__construct($columns);
    }

    /**
     * @param ColumnInterface[] $columns
     */
    private function assertTableColumnsCount(array $columns): void
    {
        if (count($columns) > self::MAX_TABLE_COLUMNS) {
            throw new ColumnException(
                sprintf('Too many columns. Maximum is %s columns.', self::MAX_TABLE_COLUMNS),
                ColumnException::STRING_CODE_TO_MANY_COLUMNS
            );
        }
    }
}
