<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport;

class ImportOptions
{

    public const SKIP_FIRST_LINE = 1;

    /** @var string */
    private $schema;

    /** @var string */
    private $tableName;

    /** @var boolean */
    private $useTimestamp;

    /** @var array */
    private $convertEmptyValuesToNull = [];

    /** @var array */
    private $columns = [];

    /** @var bool */
    private $isIncremental;

    /** @var int */
    private $numberOfIgnoredLines = 0;

    public function __construct(string $schema, string $tableName)
    {
        $this->schema = $schema;
        $this->tableName = $tableName;
        $this->isIncremental = false;
        $this->useTimestamp = false; //TODO: verify if default value should be false ???
    }

    public function getSchema(): string
    {
        return $this->schema;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }

    public function useTimestamp(): bool
    {
        return $this->useTimestamp;
    }

    public function setUseTimestamp(bool $useTimestamp): void
    {
        $this->useTimestamp = $useTimestamp;
    }

    public function getConvertEmptyValuesToNull(): array
    {
        return $this->convertEmptyValuesToNull;
    }

    public function setConvertEmptyValuesToNull(array $convertEmptyValuesToNull): void
    {
        $this->convertEmptyValuesToNull = $convertEmptyValuesToNull;
    }

    public function getColumns(): array
    {
        return $this->columns;
    }

    public function setColumns(array $columns): void
    {
        $this->columns = $columns;
    }

    public function isIncremental(): bool
    {
        return $this->isIncremental;
    }

    public function setIsIncremental(bool $isIncremental): void
    {
        $this->isIncremental = $isIncremental;
    }

    public function getNumberOfIgnoredLines(): int
    {
        return $this->numberOfIgnoredLines;
    }

    public function setNumberOfIgnoredLines(int $numberOfIgnoredLines): void
    {
        $this->numberOfIgnoredLines = $numberOfIgnoredLines;
    }
}
