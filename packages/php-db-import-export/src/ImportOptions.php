<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport;

class ImportOptions
{
    public const SKIP_NO_LINE = 0;
    public const SKIP_FIRST_LINE = 1;

    /** @var boolean */
    private $useTimestamp;

    /** @var array */
    private $convertEmptyValuesToNull = [];

    /** @var bool */
    private $isIncremental;

    /** @var int */
    private $numberOfIgnoredLines = 0;

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
