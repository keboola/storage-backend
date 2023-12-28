<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

interface CopyAdapterInterface
{
    /**
     * @return int number of inserted rows
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        TableDefinitionInterface $destination,
        ImportOptionsInterface $importOptions,
    ): int;
}
