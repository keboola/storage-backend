<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

interface ToFinalTableImporterInterface
{
    public const TIMESTAMP_COLUMN_NAME = '_timestamp';
    public const SLICED_FILES_CHUNK_SIZE = 1000;

    public function importToTable(
        TableDefinitionInterface $stagingTableDefinition,
        TableDefinitionInterface $destinationTableDefinition,
        ImportOptionsInterface $options,
        ImportState $state
    ): Result;
}
