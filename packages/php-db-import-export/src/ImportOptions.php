<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport;

use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;

class ImportOptions
{
    public const SKIP_NO_LINE = 0;
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

    public function __construct(
        string $schema,
        string $tableName,
        array $convertEmptyValuesToNull = [],
        array $columns = [],
        bool $isIncremental = false,
        bool $useTimestamp = false,
        //TODO: verify if default value should be false ???
        int $numberOfIgnoredLines = 0
    ) {
        $this->schema = $schema;
        $this->tableName = $tableName;
        $this->useTimestamp = $useTimestamp;
        $this->convertEmptyValuesToNull = $convertEmptyValuesToNull;
        $this->columns = $columns;
        $this->isIncremental = $isIncremental;
        $this->numberOfIgnoredLines = $numberOfIgnoredLines;
    }

    public function getColumns(): array
    {
        return $this->columns;
    }

    public function getConvertEmptyValuesToNull(): array
    {
        return $this->convertEmptyValuesToNull;
    }

    public function getNumberOfIgnoredLines(): int
    {
        return $this->numberOfIgnoredLines;
    }

    public function getSchema(): string
    {
        return $this->schema;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }

    public function getTargetTableWithScheme(): string
    {
        return sprintf(
            '%s.%s',
            QuoteHelper::quoteIdentifier($this->schema),
            QuoteHelper::quoteIdentifier($this->tableName),
        );
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
