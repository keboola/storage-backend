<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\SourceStorage;

use Keboola\Db\ImportExport\ImportOptions;

interface BackendImportAdapterInterface
{
    public function __construct(SourceInterface $source);

    public function getCopyCommands(
        ImportOptions $importOptions,
        string $stagingTableName
    ): array;
}
