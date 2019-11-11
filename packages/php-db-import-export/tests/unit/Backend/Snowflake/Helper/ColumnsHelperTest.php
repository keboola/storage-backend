<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\Helper;

use Keboola\Db\ImportExport\Backend\Snowflake\Helper\ColumnsHelper;
use PHPUnit\Framework\TestCase;

class ColumnsHelperTest extends TestCase
{
    public function testGetColumnsStringDefault(): void
    {
        $colsString = ColumnsHelper::getColumnsString(['col1', 'col2']);
        self::assertEquals('"col1", "col2"', $colsString);
    }

    public function testGetColumnsStringDelimiter(): void
    {
        $colsString = ColumnsHelper::getColumnsString(['col1', 'col2'], ';');
        self::assertEquals('"col1";"col2"', $colsString);
    }

    public function testGetColumnsStringTableAlias(): void
    {
        $colsString = ColumnsHelper::getColumnsString([
            'col1',
            'col2',
        ], ', ', 'a');
        self::assertEquals('a."col1", a."col2"', $colsString);
    }
}
