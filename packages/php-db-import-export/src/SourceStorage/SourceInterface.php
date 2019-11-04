<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\SourceStorage;

use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ImporterInterface;

interface SourceInterface
{
    public function getBackendImportAdapter(ImporterInterface $importer): BackendImportAdapterInterface;
}
