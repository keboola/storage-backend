<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\SourceStorage;

interface ImporterInterface
{
    public const SLICED_FILES_CHUNK_SIZE = 1000;

    public function importTable(
        ImportOptions $options,
        SourceStorage\SourceInterface $source
    ): Result;
}
