<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Synapse\Helper;

use Keboola\Db\ImportExport\Backend\Synapse\Helper\BackendHelper;
use PHPUnit\Framework\TestCase;

class BackendHelperTest extends TestCase
{
    public function testGenerateStagingTableName(): void
    {
        $tableName = BackendHelper::generateTempTableName();
        self::assertStringContainsString('#__temp_csvimport', $tableName);
    }
}
