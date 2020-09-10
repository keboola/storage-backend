<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport;

use Keboola\Db\ImportExport\Backend\BackendHelper;

class ExportOptions implements ExportOptionsInterface
{
    /**
     * @var bool
     */
    private $isCompressed;

    /** @var string */
    private $exportId;

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
