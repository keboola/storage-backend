<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport;

class ImportOptions implements ImportOptionsInterface
{
    private bool $useTimestamp;

    /** @var string[] */
    private array $convertEmptyValuesToNull;

    private bool $isIncremental;

    private int $numberOfIgnoredLines;

    /** @var self::USING_TYPES_* */
    private string $usingTypes;

    /**
     * @param string[] $convertEmptyValuesToNull
     * @param self::USING_TYPES_* $usingTypes
     */
    public function __construct(
        array $convertEmptyValuesToNull = [],
        bool $isIncremental = false,
        bool $useTimestamp = false,
        int $numberOfIgnoredLines = self::SKIP_NO_LINE,
        string $usingTypes = self::USING_TYPES_STRING
    ) {
        $this->useTimestamp = $useTimestamp;
        $this->convertEmptyValuesToNull = $convertEmptyValuesToNull;
        $this->isIncremental = $isIncremental;
        $this->numberOfIgnoredLines = $numberOfIgnoredLines;
        $this->usingTypes = $usingTypes;
    }

    /**
     * @return string[]
     */
    public function getConvertEmptyValuesToNull(): array
    {
        return $this->convertEmptyValuesToNull;
    }

    public function getNumberOfIgnoredLines(): int
    {
        return $this->numberOfIgnoredLines;
    }

    public function isIncremental(): bool
    {
        return $this->isIncremental;
    }

    public function useTimestamp(): bool
    {
        return $this->useTimestamp;
    }

    public function usingUserDefinedTypes(): bool
    {
        return $this->usingTypes === self::USING_TYPES_USER;
    }
}
