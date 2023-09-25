<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Keboola\Db\ImportExport\ImportOptions;

class SnowflakeImportOptions extends ImportOptions
{
    /** @var self::SAME_TABLES_* */
    private bool $requireSameTables;

    /** @var self::NULL_MANIPULATION_* */
    private bool $nullManipulation;

    /**
     * @param string[] $convertEmptyValuesToNull
     * @param self::SAME_TABLES_* $requireSameTables
     * @param self::NULL_MANIPULATION_* $nullManipulation
     * @param string[] $ignoreColumns
     */
    public function __construct(
        array $convertEmptyValuesToNull = [],
        bool $isIncremental = false,
        bool $useTimestamp = false,
        int $numberOfIgnoredLines = 0,
        bool $requireSameTables = self::SAME_TABLES_NOT_REQUIRED,
        bool $nullManipulation = self::NULL_MANIPULATION_ENABLED,
        array $ignoreColumns = [],
    ) {
        parent::__construct(
            $convertEmptyValuesToNull,
            $isIncremental,
            $useTimestamp,
            $numberOfIgnoredLines,
            $requireSameTables === self::SAME_TABLES_REQUIRED ? self::USING_TYPES_USER : self::USING_TYPES_STRING,
            $ignoreColumns
        );
        $this->requireSameTables = $requireSameTables;
        $this->nullManipulation = $nullManipulation;
    }

    public function isRequireSameTables(): bool
    {
        return $this->requireSameTables === self::SAME_TABLES_REQUIRED;
    }

    public function isNullManipulationEnabled(): bool
    {
        return $this->nullManipulation === self::NULL_MANIPULATION_ENABLED;
    }
}
