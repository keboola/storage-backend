<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport;

interface ExportOptionsInterface
{
    public function getExportId(): string;

    public function isCompressed(): bool;

    public function generateManifest(): bool;

    /**
     * @return string[]
     */
    public function features(): array;
}
