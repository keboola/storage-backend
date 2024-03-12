<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Exasol;

use Keboola\Db\ImportExport\ImportOptions;

class ExasolImportOptions extends ImportOptions
{
    public const TABLE_TYPES_CAST = 'TABLE_HAS_TYPES_DEFINED';
    public const TABLE_TYPES_PRESERVE = 'TABLE_TYPES_PRESERVE';

    /** @var self::TABLE_TYPES_* */
    private $castValueTypes;

    /**
     * @param string[] $convertEmptyValuesToNull
     * @param self::TABLE_TYPES_* $castValueTypes
     */
    public function __construct(
        array $convertEmptyValuesToNull = [],
        bool $isIncremental = false,
        bool $useTimestamp = false,
        int $numberOfIgnoredLines = 0,
        string $castValueTypes = self::TABLE_TYPES_PRESERVE,
    ) {
        parent::__construct(
            convertEmptyValuesToNull: $convertEmptyValuesToNull,
            isIncremental: $isIncremental,
            useTimestamp: $useTimestamp,
            numberOfIgnoredLines: $numberOfIgnoredLines,
            importAsNull: [], // Exasol does not support importAsNull now
        );
        $this->castValueTypes = $castValueTypes;
    }

    public function getCastValueTypes(): bool
    {
        return $this->castValueTypes === self::TABLE_TYPES_CAST;
    }
}
