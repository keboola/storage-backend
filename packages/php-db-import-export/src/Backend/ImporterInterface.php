<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;

interface ImporterInterface
{
    public const SLICED_FILES_CHUNK_SIZE = 1000;

    public function importTable(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptions $options
    ): Result;

    public function setAdapters(array $adapters): void;
}
