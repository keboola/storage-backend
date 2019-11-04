<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\SourceStorage\SourceInterface;

interface BackendImportAdapterInterface
{
    public function __construct(SourceInterface $source);

    public function getCopyCommands(
        ImportOptions $importOptions,
        string $stagingTableName
    ): array;
}
