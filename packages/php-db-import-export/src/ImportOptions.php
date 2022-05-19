<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport;

class ImportOptions implements ImportOptionsInterface
{
    public const SKIP_NO_LINE = 0;
    public const SKIP_FIRST_LINE = 1;

    private bool $useTimestamp;

    /** @var string[] */
    private array $convertEmptyValuesToNull = [];

    private bool $isIncremental;

    private int $numberOfIgnoredLines = 0;

    /**
     * @param string[] $convertEmptyValuesToNull
     */
    public function __construct(
        array $convertEmptyValuesToNull = [],
        bool $isIncremental = false,
        bool $useTimestamp = false,
        int $numberOfIgnoredLines = 0
    ) {
        $this->useTimestamp = $useTimestamp;
        $this->convertEmptyValuesToNull = $convertEmptyValuesToNull;
        $this->isIncremental = $isIncremental;
        $this->numberOfIgnoredLines = $numberOfIgnoredLines;
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
}
