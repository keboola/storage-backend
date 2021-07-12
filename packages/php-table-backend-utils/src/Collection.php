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

    /**
     * @param array<mixed> $data
     * @param string $key
     * @return array<mixed>
     */
    public static function extractByKey(array $data, string $key): array
    {
        return array_map(static function ($record) use ($key) {
            return trim($record[$key]);
        }, $data);
    }
}
