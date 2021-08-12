<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Exasol\Helper;

use Keboola\Db\ImportExport\Backend\Exasol\Helper\BackendHelper;
use PHPUnit\Framework\TestCase;

class BackendHelperTest extends TestCase
{
    public function testGenerateStagingTableName(): void
    {
        $tableName = BackendHelper::generateTempTableName();
        self::assertContains('__temp_csvimport', $tableName);
    }
}
