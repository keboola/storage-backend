<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column;

/**
 * @implements \Iterator<ColumnInterface>
 */
final class ColumnIterator implements \Iterator, \Countable
{
    /** @var int */
    private $position = 0;

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
     * @return string[]
     */
    public function getColumnsNames(): array
    {
        return array_map(static function (ColumnInterface $col) {
            return $col->getColumnName();
        }, $this->columns);
    }

    public function hasColumnWithName(string $name): bool
    {
        foreach ($this->columns as $col) {
            if ($col->getColumnName() === $name) {
                return true;
            }
        }
        return false;
    }

    public function rewind(): void
    {
        $this->position = 0;
    }

    public function current(): ColumnInterface
    {
        return $this->columns[$this->position];
    }

    public function key(): int
    {
        return $this->position;
    }

    public function next(): void
    {
        ++$this->position;
    }

    public function valid(): bool
    {
        return isset($this->columns[$this->position]);
    }

    public function count(): int
    {
        return count($this->columns);
    }
}
