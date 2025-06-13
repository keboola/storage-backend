<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Datatype\Definition\Common;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeException;
use Keboola\Db\ImportExport\Exception\ColumnsMismatchException;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

class Assert
{
    /**
     * Ignore other assert options
     * this would be better as default but it would be BC break
     */
    public const ASSERT_MINIMAL = 0;
    public const ASSERT_LENGTH = 1;

    public const ASSERT_STRICT_LENGTH = 2;

    /**
     * @param string[] $ignoreSourceColumns
     * @param string[] $simpleLengthTypes - list of types where length is represented by single number
     * @param string[] $complexLengthTypes
     * - list of numeric types where length is presented by <scale:int>,<precision:int>
     * @throws ColumnsMismatchException
     */
    public static function assertSameColumns(
        ColumnCollection $source,
        ColumnCollection $destination,
        array $ignoreSourceColumns = [],
        array $simpleLengthTypes = [],
        array $complexLengthTypes = [],
        int $assertOptions = self::ASSERT_LENGTH,
    ): void {
        $it0 = $source->getIterator();
        $it1 = $destination->getIterator();
        while ($it0->valid() || $it1->valid()) {
            if ($it0->valid() && in_array($it0->current()->getColumnName(), $ignoreSourceColumns, true)) {
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

                if ($assertOptions & self::ASSERT_LENGTH) {
                    self::assertLengthMismatch(
                        $sourceCol,
                        $destCol,
                        $simpleLengthTypes,
                        $complexLengthTypes,
                    );
                }

                if ($assertOptions & self::ASSERT_STRICT_LENGTH) {
                    self::assertLengthMismatchStrict(
                        $sourceCol,
                        $destCol,
                    );
                }

                if ($assertOptions & self::ASSERT_STRICT_LENGTH) {
                    self::assertLengthMismatchStrict(
                        $sourceCol,
                        $destCol,
                    );
                }
            } else {
                throw ColumnsMismatchException::createColumnsCountMismatch($source, $destination);
            }

            $it0->next();
            $it1->next();
        }
    }

    public static function assertPrimaryKeys(
        TableDefinitionInterface $source,
        TableDefinitionInterface $destination,
    ): void {
        $sourcePrimaryKeys = $source->getPrimaryKeysNames();
        $destinationPrimaryKeys = $destination->getPrimaryKeysNames();
        sort($sourcePrimaryKeys);
        sort($destinationPrimaryKeys);
        if ($sourcePrimaryKeys !== $destinationPrimaryKeys) {
            throw ColumnsMismatchException::createPrimaryKeysColumnsMismatch(
                $sourcePrimaryKeys,
                $destinationPrimaryKeys,
            );
        }
    }

    private static function isLengthMismatchSimpleLength(Common $sourceDef, Common $destDef): bool
    {
        $isSourceNumeric = is_numeric($sourceDef->getLength());
        $isDestNumeric = is_numeric($destDef->getLength());
        if ($isSourceNumeric && $isDestNumeric) {
            return (int) $sourceDef->getLength() > (int) $destDef->getLength();
        }
        if ($isSourceNumeric !== $isDestNumeric) {
            // if one is numeric but other not => mismatch
            return true;
        }
        return false;
    }

    private static function isLengthMismatchComplexLength(
        int $sourcePrecision,
        int $sourceScale,
        int $destinationPrecision,
        int $destinationScale,
    ): bool {
        return $sourcePrecision > $destinationPrecision || $sourceScale > $destinationScale;
    }

    private static function isMismatchComplexLength(Common $sourceDef, Common $destDef): bool
    {
        assert($sourceDef->getLength() !== null); // both will never fail this is checked in assertLengthMismatch
        assert($destDef->getLength() !== null);
        $sourceLength = explode(',', $sourceDef->getLength());
        $destLength = explode(',', $destDef->getLength());
        $isSourceComplex = count($sourceLength) === 2;
        $isDestComplex = count($destLength) === 2;
        return match (true) {
            $isSourceComplex && $isDestComplex => self::isLengthMismatchComplexLength(
                sourcePrecision: (int) $sourceLength[0],
                sourceScale: (int) $sourceLength[1],
                destinationPrecision: (int) $destLength[0],
                destinationScale: (int) $destLength[1],
            ),
            $isSourceComplex !== $isDestComplex => false,
            default => self::isLengthMismatchSimpleLength(
                $sourceDef,
                $destDef,
            ),
        };
    }

    /**
     * checks if destination length is compatible (same or bigger) with source length.
     * Examples:
     *
     * With simple type and no simpleLengthTypes defined:
     * - source VARCHAR(255) -> destination VARCHAR(255) is OK;
     * - source VARCHAR(255) -> destination VARCHAR(1000) fail;
     * - source VARCHAR(1000) -> destination VARCHAR(255) fail;
     * With complex type and no complexLengthTypes defined:
     * - source DECIMAL(22,2) -> destination DECIMAL(22,2) is OK;
     * - source DECIMAL(20,2) -> destination DECIMAL(22,2) fails;
     * - source DECIMAL(22,2) -> destination DECIMAL(22,3) fails;
     * - source DECIMAL(22,2) -> destination DECIMAL(20,2) fails;
     * - source DECIMAL(22,2) -> destination DECIMAL(22,1) fails;
     *
     * With simple type and simpleLengthTypes defined:
     * - source VARCHAR(255) -> destination VARCHAR(255) is OK;
     * - source VARCHAR(255) -> destination VARCHAR(1000) is OK;
     * - source VARCHAR(1000) -> destination VARCHAR(255) fail;
     * With complex type and complexLengthTypes defined:
     * - source DECIMAL(22,2) -> destination DECIMAL(22,2) is OK;
     * - source DECIMAL(20,2) -> destination DECIMAL(22,2) is OK;
     * - source DECIMAL(22,2) -> destination DECIMAL(22,3) is OK;
     * - source DECIMAL(22,2) -> destination DECIMAL(20,2) fails;
     * - source DECIMAL(22,2) -> destination DECIMAL(22,1) fails;
     *
     * @param string[] $simpleLengthTypes
     * @param string[] $complexLengthTypes
     */
    private static function assertLengthMismatch(
        ColumnInterface $sourceCol,
        ColumnInterface $destCol,
        array $simpleLengthTypes = [],
        array $complexLengthTypes = [],
    ): void {
        /** @var Common $sourceDef */
        $sourceDef = $sourceCol->getColumnDefinition();
        /** @var Common $destDef */
        $destDef = $destCol->getColumnDefinition();

        if ($sourceDef->getLength() === null || $destDef->getLength() === null) {
            // if any of the lengths is null do simple equals check
            $isLengthMismatch = $sourceDef->getLength() !== $destDef->getLength();
        } else {
            $isSimpleLengthType = in_array(strtoupper($sourceDef->getType()), $simpleLengthTypes, true);
            $isComplexLengthType = in_array(strtoupper($sourceDef->getType()), $complexLengthTypes, true);
            $isLengthMismatch = match (true) {
                $isSimpleLengthType => self::isLengthMismatchSimpleLength($sourceDef, $destDef),
                $isComplexLengthType => self::isMismatchComplexLength($sourceDef, $destDef),
                default => $sourceDef->getLength() !== $destDef->getLength(),
            };
        }

        if ($isLengthMismatch) {
            throw ColumnsMismatchException::createColumnsMismatch($sourceCol, $destCol);
        }
    }

    private static function assertLengthMismatchStrict(
        ColumnInterface $sourceCol,
        ColumnInterface $destCol,
    ): void {
        /** @var Common $sourceDef */
        $sourceDef = $sourceCol->getColumnDefinition();
        /** @var Common $destDef */
        $destDef = $destCol->getColumnDefinition();

        if ($sourceDef->getLength() !== $destDef->getLength()) {
            throw ColumnsMismatchException::createColumnsMismatch($sourceCol, $destCol);
        }
    }
}
