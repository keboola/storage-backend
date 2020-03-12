<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport;

use Keboola\Db\ImportExport\Backend\Helper\BackendHelper;

class ExportOptions
{
    /**
     * @var bool
     */
    private $isCompresed;

    /** @var string */
    private $exportId;

    public function __construct(
        bool $isCompresed = false
    ) {
        $this->isCompresed = $isCompresed;
        $this->exportId = BackendHelper::generateRandomExportPrefix();
    }

    public function getExportId(): string
    {
        return $this->exportId;
    }

    public function isCompresed(): bool
    {
        return $this->isCompresed;
    }
}
