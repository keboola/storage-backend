<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport;

use Keboola\Db\ImportExport\Backend\Helper\BackendHelper;

class ExportOptions implements ExportOptionsInterface
{
    private bool $isCompressed;

    private string $exportId;

    public function __construct(
        bool $isCompressed = false
    ) {
        $this->isCompressed = $isCompressed;
        $this->exportId = BackendHelper::generateRandomExportPrefix();
    }

    public function getExportId(): string
    {
        return $this->exportId;
    }

    public function isCompressed(): bool
    {
        return $this->isCompressed;
    }
}
