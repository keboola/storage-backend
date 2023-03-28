<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Datatype\Definition\Common;
use Keboola\Db\ImportExport\Exception\ColumnsMismatchException;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;

class Assert
{
    /**
     * @param string[] $ignoreColumns
     * @throws ColumnsMismatchException
     */
    public static function assertSameColumns(
        ColumnCollection $source,
        ColumnCollection $destination,
        array $ignoreColumns = []
    ): void {
        $it0 = $source->getIterator();
        $it1 = $destination->getIterator();
        while ($it0->valid() || $it1->valid()) {
            if (in_array($it0->current()->getColumnName(), $ignoreColumns, true)) {
                $it0->next();
                if (!$it0->valid() && !$it1->valid()) {
                    break;
                }
            }
            if ($it0->valid() && $it1->valid()) {
                /** @var ColumnInterface $sourceCol */
                $sourceCol = $it0->current();
                /** @var ColumnInterface $destCol */
                $destCol = $it1->current();
                if ($sourceCol->getColumnName() !== $destCol->getColumnName()) {
                    throw ColumnsMismatchException::createColumnsNamesMismatch($sourceCol, $destCol);
                }
                /** @var Common $sourceDef */
                $sourceDef = $sourceCol->getColumnDefinition();
                /** @var Common $destDef */
                $destDef = $destCol->getColumnDefinition();

                if ($sourceDef->getType() !== $destDef->getType()) {
                    throw ColumnsMismatchException::createColumnsMismatch($sourceCol, $destCol);
                }

                $isLengthMismatch = $sourceDef->getLength() !== $destDef->getLength();

                if ($isLengthMismatch) {
                    throw ColumnsMismatchException::createColumnsMismatch($sourceCol, $destCol);
                }
            } else {
                throw ColumnsMismatchException::createColumnsCountMismatch($source, $destination);
            }

            $it0->next();
            $it1->next();
        }
    }
}
