<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column;

use Keboola\TableBackendUtils\Collection;

/**
 * @extends Collection<ColumnInterface>
 */
final class ColumnCollection extends Collection
{
    /**
     * @param ColumnInterface[] $columns
     */
    public function __construct(array $columns)
    {
        parent::__construct($columns);
    }
}
