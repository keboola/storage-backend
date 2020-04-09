<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column;

use Countable;
use Generator;
use IteratorAggregate;
use Keboola\TableBackendUtils\ColumnException;

/**
 * @implements \IteratorAggregate<ColumnInterface>
 */
final class ColumnCollection implements IteratorAggregate, Countable
{
    public const MAX_TABLE_COLUMNS = 1024;

    /** @var ColumnInterface[] */
    private $columns;

    /**
     * @param ColumnInterface[] $columns
     */
    public function __construct(array $columns)
    {
        $this->assertTableColumnsCount($columns);
        $this->columns = $columns;
    }

    private function assertTableColumnsCount(array $columns): void
    {
        if (count($columns) > self::MAX_TABLE_COLUMNS) {
            throw new ColumnException(
                sprintf('Too many columns. Maximum is %s columns.', self::MAX_TABLE_COLUMNS),
                ColumnException::STRING_CODE_TO_MANY_COLUMNS
            );
        }
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
