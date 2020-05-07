<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils;

use Countable;
use Generator;
use IteratorAggregate;

/**
 * @implements \IteratorAggregate<Item>
 * @template Item of CollectionItemInterface
 */
class Collection implements IteratorAggregate, Countable
{

    /** @var Item[] */
    protected $items;

    /**
     * @param Item[] $items
     */
    public function __construct(array $items)
    {
        $this->items = $items;
    }

    /**
     * @return Generator<Item>
     */
    public function getIterator(): Generator
    {
        foreach ($this->items as $item) {
            yield $item;
        }
    }

    public function count(): int
    {
        return count($this->items);
    }
}
