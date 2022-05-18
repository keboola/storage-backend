<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse;

use Keboola\Db\ImportExport\ImportOptions;

class SynapseImportOptions extends ImportOptions
{
    public const CREDENTIALS_MANAGED_IDENTITY = 'MANAGED_IDENTITY';
    public const CREDENTIALS_SAS = 'SAS';

    public const TABLE_TYPES_CAST = 'TABLE_HAS_TYPES_DEFINED';
    public const TABLE_TYPES_PRESERVE = 'TABLE_TYPES_PRESERVE';

    public const SAME_TABLES_REQUIRED = true;
    public const SAME_TABLES_NOT_REQUIRED = false;

    /** @var self::CREDENTIALS_* */
    private string $importCredentialsType;

    /** @var self::TABLE_TYPES_* */
    private $castValueTypes;

    /** @var self::SAME_TABLES_* */
    private bool $requireSameTables;

    /**
     * @param string[] $convertEmptyValuesToNull
     * @param self::CREDENTIALS_* $importCredentialsType
     * @param self::TABLE_TYPES_* $castValueTypes
     * @param self::SAME_TABLES_* $requireSameTables
     */
    public function __construct(
        array $convertEmptyValuesToNull = [],
        bool $isIncremental = false,
        bool $useTimestamp = false,
        int $numberOfIgnoredLines = 0,
        string $importCredentialsType = self::CREDENTIALS_SAS,
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
        $this->castValueTypes = $castValueTypes;
        $this->requireSameTables = $requireSameTables;
    }

    public function getImportCredentialsType(): string
    {
        return $this->importCredentialsType;
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
