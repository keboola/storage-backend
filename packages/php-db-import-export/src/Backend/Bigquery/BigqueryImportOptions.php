<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery;

use Keboola\Db\ImportExport\ImportOptions;
use Keboola\TableBackendUtils\Connection\Bigquery\Session;

class BigqueryImportOptions extends ImportOptions
{
    private ?Session $session;

    /**
     * @param string[] $convertEmptyValuesToNull
     * @param self::USING_TYPES_* $usingTypes
     * @param string[] $importAsNull
     * @param string[] $features
     * @param string[] $ignoreColumns
     */
    public function __construct(
        array $convertEmptyValuesToNull = [],
        bool $isIncremental = false,
        bool $useTimestamp = false,
        int $numberOfIgnoredLines = self::SKIP_NO_LINE,
        string $usingTypes = self::USING_TYPES_STRING,
        ?Session $session = null,
        array $importAsNull = self::DEFAULT_IMPORT_AS_NULL,
        array $features = [],
        array $ignoreColumns = [],
    ) {
        parent::__construct(
            convertEmptyValuesToNull: $convertEmptyValuesToNull,
            isIncremental: $isIncremental,
            useTimestamp: $useTimestamp,
            numberOfIgnoredLines: $numberOfIgnoredLines,
            usingTypes: $usingTypes,
            ignoreColumns: $ignoreColumns,
            importAsNull: $importAsNull,
            features: $features,
        );
        $this->session = $session;
    }

    public function getSession(): ?Session
    {
        return $this->session;
    }
}
