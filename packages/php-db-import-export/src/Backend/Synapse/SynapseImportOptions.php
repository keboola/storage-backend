<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Synapse;

use Keboola\Db\ImportExport\ImportOptions;

class SynapseImportOptions extends ImportOptions
{
    public const CREDENTIALS_SAS = 'SAS';
    public const CREDENTIALS_MANAGED_IDENTITY = 'MANAGED_IDENTITY';

    /** @var string */
    private $importCredentialsType;

    public function __construct(
        array $convertEmptyValuesToNull = [],
        bool $isIncremental = false,
        bool $useTimestamp = false,
        int $numberOfIgnoredLines = 0,
        string $importCredentialsType = self::CREDENTIALS_SAS
    ) {
        parent::__construct(
            $convertEmptyValuesToNull,
            $isIncremental,
            $useTimestamp,
            $numberOfIgnoredLines
        );
        $this->importCredentialsType = $importCredentialsType;
    }

    public function getImportCredentialsType(): string
    {
        return $this->importCredentialsType;
    }
}
