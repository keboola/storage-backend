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
     */
    public function __construct(
        array $convertEmptyValuesToNull = [],
        bool $isIncremental = false,
        bool $useTimestamp = false,
        int $numberOfIgnoredLines = self::SKIP_NO_LINE,
        string $usingTypes = self::USING_TYPES_STRING,
        ?Session $session = null
    ) {
        parent::__construct(
            $convertEmptyValuesToNull,
            $isIncremental,
            $useTimestamp,
            $numberOfIgnoredLines,
            $usingTypes
        );
        $this->session = $session;
    }

    public function getSession(): ?Session
    {
        return $this->session;
    }
}
