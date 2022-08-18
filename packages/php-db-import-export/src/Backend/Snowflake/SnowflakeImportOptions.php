<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Keboola\Db\ImportExport\ImportOptions;

class SnowflakeImportOptions extends ImportOptions
{
    public const SAME_TABLES_REQUIRED = true;
    public const SAME_TABLES_NOT_REQUIRED = false;

    /** @var self::SAME_TABLES_* */
    private bool $requireSameTables;

    /**
     * @param string[] $convertEmptyValuesToNull
     * @param self::SAME_TABLES_* $requireSameTables
     */
    public function __construct(
        array $convertEmptyValuesToNull = [],
        bool $isIncremental = false,
        bool $useTimestamp = false,
        int $numberOfIgnoredLines = 0,
        bool $requireSameTables = self::SAME_TABLES_NOT_REQUIRED
    ) {
        parent::__construct($convertEmptyValuesToNull, $isIncremental, $useTimestamp, $numberOfIgnoredLines);
        $this->requireSameTables = $requireSameTables;
    }

    /**
     * @return self::SAME_TABLES_*
     */
    public function isRequireSameTables(): bool
    {
        return $this->requireSameTables;
    }
}
