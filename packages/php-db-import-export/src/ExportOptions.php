<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport;

class ExportOptions
{
    /**
     * @var bool
     */
    private $isCompresed;

    public function __construct(
        bool $isCompresed = false
    ) {
        $this->isCompresed = $isCompresed;
    }

    public function isCompresed(): bool
    {
        return $this->isCompresed;
    }
}
