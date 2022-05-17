<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse;

use Keboola\Db\ImportExport\ImportOptions;

class SynapseImportOptions extends ImportOptions
{
    public const CREDENTIALS_MANAGED_IDENTITY = 'MANAGED_IDENTITY';
    public const CREDENTIALS_SAS = 'SAS';

    /** @deprecated staging table is created by user */
    public const TEMP_TABLE_HEAP = 'HEAP';
    /** @deprecated staging table is created by user */
    public const TEMP_TABLE_HEAP_4000 = 'HEAP4000';
    /** @deprecated staging table is created by user */
    public const TEMP_TABLE_COLUMNSTORE = 'COLUMNSTORE';
    /** @deprecated staging table is created by user */
    public const TEMP_TABLE_CLUSTERED_INDEX = 'CLUSTERED_INDEX';

    public const DEDUP_TYPE_CTAS = 'CTAS';
    /** @deprecated use DEDUP_TYPE_CTAS */
    public const DEDUP_TYPE_TMP_TABLE = 'TMP_TABLE';

    public const TABLE_TYPES_CAST = 'TABLE_HAS_TYPES_DEFINED';
    public const TABLE_TYPES_PRESERVE = 'TABLE_TYPES_PRESERVE';

    public const SAME_TABLES_REQUIRED = true;
    public const SAME_TABLES_NOT_REQUIRED = false;

    /** @var self::CREDENTIALS_* */
    private string $importCredentialsType;

    /** @var self::TEMP_TABLE_* */
    private string $tempTableType;

    /** @var self::DEDUP_TYPE_* */
    private string $dedupType;

    /** @var self::TABLE_TYPES_* */
    private $castValueTypes;

    /** @var self::SAME_TABLES_* */
    private bool $requireSameTables;

    /**
     * @param string[] $convertEmptyValuesToNull
     * @param self::CREDENTIALS_* $importCredentialsType
     * @param self::TEMP_TABLE_* $tempTableType @deprecated staging table is created by user
     * @param self::DEDUP_TYPE_* $dedupType @deprecated CTAS is always used
     * @param self::TABLE_TYPES_* $castValueTypes
     * @param self::SAME_TABLES_* $requireSameTables
     */
    public function __construct(
        array $convertEmptyValuesToNull = [],
        bool $isIncremental = false,
        bool $useTimestamp = false,
        int $numberOfIgnoredLines = 0,
        string $importCredentialsType = self::CREDENTIALS_SAS,
        string $tempTableType = self::TEMP_TABLE_HEAP,
        string $dedupType = self::DEDUP_TYPE_TMP_TABLE,
        string $castValueTypes = self::TABLE_TYPES_PRESERVE,
        bool $requireSameTables = self::SAME_TABLES_NOT_REQUIRED
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
        $this->castValueTypes = $castValueTypes;
        $this->requireSameTables = $requireSameTables;
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

    public function getCastValueTypes(): bool
    {
        return $this->castValueTypes === self::TABLE_TYPES_CAST;
    }

    public function areSameTablesRequired(): bool
    {
        return $this->requireSameTables === self::SAME_TABLES_REQUIRED;
    }
}
