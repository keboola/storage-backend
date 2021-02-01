<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse;

use Keboola\Db\ImportExport\ImportOptions;

class SynapseImportOptions extends ImportOptions
{
    public const CREDENTIALS_MANAGED_IDENTITY = 'MANAGED_IDENTITY';
    public const CREDENTIALS_SAS = 'SAS';

    public const TEMP_TABLE_HEAP = 'HEAP';
    public const TEMP_TABLE_HEAP_4000 = 'HEAP4000';
    public const TEMP_TABLE_COLUMNSTORE = 'COLUMNSTORE';
    public const TEMP_TABLE_CLUSTERED_INDEX = 'CLUSTERED_INDEX';

    public const DEDUP_TYPE_CTAS = 'CTAS';
    public const DEDUP_TYPE_TMP_TABLE = 'TMP_TABLE';

    /** @var string */
    private $importCredentialsType;

    /** @var string */
    private $tempTableType;

    /** @var string */
    private $dedupType;

    public function __construct(
        array $convertEmptyValuesToNull = [],
        bool $isIncremental = false,
        bool $useTimestamp = false,
        int $numberOfIgnoredLines = 0,
        string $importCredentialsType = self::CREDENTIALS_SAS,
        string $tempTableType = self::TEMP_TABLE_HEAP,
        string $dedupType = self::DEDUP_TYPE_TMP_TABLE
    ) {
        parent::__construct(
            $convertEmptyValuesToNull,
            $isIncremental,
            $useTimestamp,
            $numberOfIgnoredLines
        );
        $this->importCredentialsType = $importCredentialsType;
        $this->tempTableType = $tempTableType;
        $this->dedupType = $dedupType;
    }

    public function getImportCredentialsType(): string
    {
        return $this->importCredentialsType;
    }

    public function getTempTableType(): string
    {
        return $this->tempTableType;
    }

    public function useOptimizedDedup(): bool
    {
        return $this->dedupType === self::DEDUP_TYPE_CTAS;
    }
}
