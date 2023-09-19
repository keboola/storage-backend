<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Exception;
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
     * @var WeakMap<ColumnInterface,ColumnInterface>
     */
    private WeakMap $map;

    /**
     * @param string[] $ignoreColumns
     */
    public function __construct(
        private readonly ColumnCollection $source,
        private readonly ColumnCollection $destination,
        private readonly array $ignoreColumns = [],
    ) {
        $this->map = new WeakMap();
        $this->buildMap();
    }

    /**
     * @param string[] $ignoreColumns
     */
    public static function createForTables(
        TableDefinitionInterface $source,
        TableDefinitionInterface $destination,
        array $ignoreColumns = [],
    ): self {
        return new self(
            $source->getColumnsDefinitions(),
            $destination->getColumnsDefinitions(),
            $ignoreColumns
        );
    }

    private function buildMap(): void
    {
        $it0 = $this->source->getIterator();
        $it1 = $this->destination->getIterator();
        while ($it0->valid() || $it1->valid()) {
            if ($it0->valid() && in_array($it0->current()->getColumnName(), $this->ignoreColumns, true)) {
                $it0->next();
                if (!$it0->valid() && !$it1->valid()) {
                    break;
                }
            }
            if ($it1->valid() && in_array($it1->current()->getColumnName(), $this->ignoreColumns, true)) {
                $it1->next();
                if (!$it0->valid() && !$it1->valid()) {
                    break;
                }
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

    public function getDestination(ColumnInterface $source): ColumnInterface
    {
        $destination = $this->map[$source];
        if (!$destination instanceof ColumnInterface) {
            // this can happen only when class is used with different source and destination tables instances
            throw new Exception(sprintf('Column "%s" not found in destination table', $source->getColumnName()));
        }
        return $destination;
    }
}
