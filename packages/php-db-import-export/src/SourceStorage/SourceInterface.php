<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\SourceStorage;

use Keboola\Db\ImportExport\SourceStorage\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ImporterInterface;

interface SourceInterface
{
    public function getBackendAdapter(ImporterInterface $importer): BackendImportAdapterInterface;
}
