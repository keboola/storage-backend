<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column;

use Countable;
use Generator;
use IteratorAggregate;

/**
 * @implements \IteratorAggregate<ColumnInterface>
 */
final class ColumnCollection implements IteratorAggregate, Countable
{
    /** @var ColumnInterface[] */
    private $columns;

    /**
     * @param ColumnInterface[] $columns
     */
    public function __construct(array $columns)
    {
        $this->columns = $columns;
    }

    /**
     * @return Generator<ColumnInterface>
     */
    public function getIterator(): Generator
    {
        foreach ($this->columns as $col) {
            yield $col;
        }
    }

    public function count(): int
    {
        return count($this->columns);
    }
}
