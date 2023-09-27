<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Error;
use Exception;
use Generator;
use Keboola\Db\ImportExport\Exception\ColumnsMismatchException;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use WeakMap;

/**
 * Class will create map of table column based on columns order
 */
final class SourceDestinationColumnMap
{
    /**
     * This mode will keep order of columns in both tables
     * Map will fail if columns count is different
     */
    public const MODE_MAP_BY_ORDER = 'order';
    /**
     * This mode will take source order and map destination columns by name
     * Map will fail if column is missing on one of the tables
     */
    public const MODE_MAP_BY_NAME = 'name';

    /**
     * @var WeakMap<ColumnInterface,ColumnInterface>
     */
    private WeakMap $map;

    /**
     * @param string[] $ignoreColumns
     * @param self::MODE_* $mode
     */
    public function __construct(
        private readonly ColumnCollection $source,
        private readonly ColumnCollection $destination,
        private readonly array $ignoreColumns = [],
        private readonly string $mode = self::MODE_MAP_BY_ORDER,
    ) {
        $this->map = new WeakMap();
        $this->buildMap();
    }

    /**
     * @param string[] $ignoreColumns
     * @param self::MODE_* $mode
     */
    public static function createForTables(
        TableDefinitionInterface $source,
        TableDefinitionInterface $destination,
        array $ignoreColumns = [],
        string $mode = self::MODE_MAP_BY_ORDER,
    ): self {
        return new self(
            $source->getColumnsDefinitions(),
            $destination->getColumnsDefinitions(),
            $ignoreColumns,
            $mode
        );
    }

    private function buildMap(): void
    {
        if ($this->mode === self::MODE_MAP_BY_ORDER) {
            $this->buildMapBasedOnOrder();
            return;
        }
        $this->buildMapBasedOnNames();
    }

    private function buildMapBasedOnOrder(): void
    {
        $it0 = $this->source->getIterator();
        $it1 = $this->destination->getIterator();
        while ($it0->valid() || $it1->valid()) {
            $it0 = $this->ignoreColumn($it0, $it1);
            if ($it0 === false) {
                break;
            }
            $it1 = $this->ignoreColumn($it1, $it0);
            if ($it1 === false) {
                break;
            }

            if ($it0->valid() && $it1->valid()) {
                /** @var ColumnInterface $sourceCol */
                $sourceCol = $it0->current();
                /** @var ColumnInterface $destCol */
                $destCol = $it1->current();
                $this->map[$sourceCol] = $destCol;
            } else {
                throw ColumnsMismatchException::createColumnsCountMismatch($this->source, $this->destination);
            }
            $it0->next();
            $it1->next();
        }
    }

    private function buildMapBasedOnNames(): void
    {
        foreach ($this->source as $sourceColumn) {
            if ($this->isColumnNameInIgnoredList($sourceColumn->getColumnName())) {
                continue;
            }
            foreach ($this->destination as $destinationColumn) {
                if ($destinationColumn->getColumnName() === $sourceColumn->getColumnName()) {
                    $this->map[$sourceColumn] = $destinationColumn;
                    continue 2;
                }
            }
            throw ColumnsMismatchException::createColumnByNameMissing($sourceColumn);
        }
    }

    public function getDestination(ColumnInterface $source): ColumnInterface
    {
        try {
            $destination = $this->map[$source];
        } catch (Error $e) {
            // this can happen only when class is used with different source and destination tables instances
            throw new Exception(sprintf('Column "%s" not found in destination table', $source->getColumnName()));
        }
        assert($destination !== null);
        return $destination;
    }

    /**
     * @param Generator<int, ColumnInterface> $it0
     * @param Generator<int, ColumnInterface> $it1
     * @return Generator<int, ColumnInterface>|false
     */
    private function ignoreColumn(Generator $it0, Generator $it1): Generator|false
    {
        if ($this->isIgnoredColumn($it0)) {
            $it0->next();
            $this->ignoreColumn($it0, $it1);
            if (!$it0->valid() && !$it1->valid()) {
                return false;
            }
        }

        return $it0;
    }

    /**
     * @param Generator<int, ColumnInterface> $it
     */
    private function isIgnoredColumn(Generator $it): bool
    {
        return $it->valid() && $this->isColumnNameInIgnoredList($it->current()->getColumnName());
    }

    private function isColumnNameInIgnoredList(string $name): bool
    {
        return in_array($name, $this->ignoreColumns, true);
    }
}
