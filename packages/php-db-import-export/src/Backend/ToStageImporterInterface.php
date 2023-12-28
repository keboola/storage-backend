<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

interface ToStageImporterInterface
{
    public const TIMESTAMP_COLUMN_NAME = '_timestamp';
    public const SLICED_FILES_CHUNK_SIZE = 1000;

    public function importToStagingTable(
        Storage\SourceInterface $source,
        TableDefinitionInterface $destinationDefinition,
        ImportOptionsInterface $options,
    ): ImportState;
}
