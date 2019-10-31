<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\SourceStorage;

interface ImporterInterface
{
    public function importTable(
        ImportOptions $options,
        SourceStorage\SourceInterface $source
    ): void;
}
