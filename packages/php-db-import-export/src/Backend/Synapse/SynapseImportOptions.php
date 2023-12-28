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

    public const TABLE_TO_TABLE_ADAPTER_INSERT_INTO = 'INSERT_INTO';
    public const TABLE_TO_TABLE_ADAPTER_CTAS = 'CTAS';

    /** @var self::CREDENTIALS_* */
    private string $importCredentialsType;

    /** @var self::TABLE_TYPES_* */
    private $castValueTypes;

    /** @var self::SAME_TABLES_* */
    private bool $requireSameTables;

    private string $tableToTableAdapter;

    /**
     * @param string[] $convertEmptyValuesToNull
     * @param self::CREDENTIALS_* $importCredentialsType
     * @param self::TABLE_TYPES_* $castValueTypes
     * @param self::SAME_TABLES_* $requireSameTables
     * @param self::TABLE_TO_TABLE_ADAPTER_* $tableToTableAdapter
     */
    public function __construct(
        array $convertEmptyValuesToNull = [],
        bool $isIncremental = false,
        bool $useTimestamp = false,
        int $numberOfIgnoredLines = 0,
        string $importCredentialsType = self::CREDENTIALS_SAS,
        string $castValueTypes = self::TABLE_TYPES_PRESERVE,
        bool $requireSameTables = self::SAME_TABLES_NOT_REQUIRED,
        string $tableToTableAdapter = self::TABLE_TO_TABLE_ADAPTER_INSERT_INTO,
    ) {
        parent::__construct(
            $convertEmptyValuesToNull,
            $isIncremental,
            $useTimestamp,
            $numberOfIgnoredLines,
            $requireSameTables === self::SAME_TABLES_REQUIRED ? self::USING_TYPES_USER : self::USING_TYPES_STRING,
        );
        $this->importCredentialsType = $importCredentialsType;
        $this->castValueTypes = $castValueTypes;
        $this->requireSameTables = $requireSameTables;
        $this->tableToTableAdapter = $tableToTableAdapter;
    }

    public function getTableToTableAdapter(): string
    {
        return $this->tableToTableAdapter;
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
