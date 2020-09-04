<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Synapse;

use Keboola\Db\ImportExport\ExportOptions;

class SynapseExportOptions extends ExportOptions
{
    public const CREDENTIALS_MASTER_KEY = 'MASTER_KEY';
    public const CREDENTIALS_MANAGED_IDENTITY = 'MANAGED_IDENTITY';

    /** @var string */
    private $exportCredentialsType;

    public function __construct(
        bool $isCompressed = false,
        string $exportCredentialsType = self::CREDENTIALS_MASTER_KEY
    ) {
        parent::__construct($isCompressed);
        $this->exportCredentialsType = $exportCredentialsType;
    }

    public function getExportCredentialsType(): string
    {
        return $this->exportCredentialsType;
    }
}
