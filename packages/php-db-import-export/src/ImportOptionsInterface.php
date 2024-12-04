<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport;

interface ImportOptionsInterface
{
    public const USING_TYPES_STRING = 'string'; // default connection tables with string type only
    public const USING_TYPES_USER = 'user'; // custom "typed" table with various types
    public const SKIP_NO_LINE = 0;
    public const SKIP_FIRST_LINE = 1;
    public const DEFAULT_IMPORT_AS_NULL = ['']; // default import empty string as null

    /** @return string[] */
    public function getConvertEmptyValuesToNull(): array;

    public function getNumberOfIgnoredLines(): int;

    public function isIncremental(): bool;

    public function useTimestamp(): bool;

    public function usingUserDefinedTypes(): bool;

    /**
     * @return string[]
     */
    public function ignoreColumns(): array;

    /**
     * List of values which are during the import converted to null
     * this option is mainly passed directly to the backend in some option
     * like NULL_IF in SNFLK
     *
     * @return string[]
     */
    public function importAsNull(): array;

    /**
     * @return string[]
     */
    public function features(): array;
}
