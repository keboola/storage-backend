<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\Helper;

use Keboola\Db\ImportExport\Backend\Snowflake\Helper\BackendHelper;
use PHPUnit\Framework\TestCase;

class BackendHelperTest extends TestCase
{
    public function testGenerateStagingTableName(): void
    {
        $tableName = BackendHelper::generateStagingTableName();
        self::assertContains('__temp_csvimport', $tableName);
    }
}
