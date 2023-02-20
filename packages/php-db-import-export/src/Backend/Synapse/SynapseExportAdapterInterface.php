<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface;

interface SynapseExportAdapterInterface extends BackendExportAdapterInterface
{
    public function __construct(Connection $connection);
}
