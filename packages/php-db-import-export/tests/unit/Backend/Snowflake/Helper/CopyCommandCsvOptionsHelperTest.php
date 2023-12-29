<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\Helper;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\CopyCommandCsvOptionsHelper;
use Keboola\Db\ImportExport\ImportOptions;
use PHPUnit\Framework\TestCase;

class CopyCommandCsvOptionsHelperTest extends TestCase
{
    public function testGetCsvCopyCommandOptions(): void
    {
        $result = CopyCommandCsvOptionsHelper::getCsvCopyCommandOptions(
            new ImportOptions(),
            new CsvOptions(),
        );

        self::assertSame([
            'FIELD_DELIMITER = \',\'',
            'FIELD_OPTIONALLY_ENCLOSED_BY = \'\"\'',
            'ESCAPE_UNENCLOSED_FIELD = NONE',
        ], $result);
    }

    public function testGetCsvCopyCommandOptionsNoEscapeNoEnclosure(): void
    {
        $result = CopyCommandCsvOptionsHelper::getCsvCopyCommandOptions(
            new ImportOptions([], false, false, 1),
            new CsvOptions(
                CsvOptions::DEFAULT_DELIMITER,
                '',
            ),
        );

        self::assertSame([
            'FIELD_DELIMITER = \',\'',
            'SKIP_HEADER = 1',
        ], $result);
    }

    public function testGetCsvCopyCommandOptionsSkipHeader(): void
    {
        $result = CopyCommandCsvOptionsHelper::getCsvCopyCommandOptions(
            new ImportOptions([], false, false, 1),
            new CsvOptions(),
        );

        self::assertSame([
            'FIELD_DELIMITER = \',\'',
            'SKIP_HEADER = 1',
            'FIELD_OPTIONALLY_ENCLOSED_BY = \'\"\'',
            'ESCAPE_UNENCLOSED_FIELD = NONE',
        ], $result);
    }

    public function testGetCsvCopyCommandOptionsUnenclosedField(): void
    {
        $result = CopyCommandCsvOptionsHelper::getCsvCopyCommandOptions(
            new ImportOptions([], false, false, 1),
            new CsvOptions(
                CsvOptions::DEFAULT_DELIMITER,
                '',
                '\\',
            ),
        );

        self::assertSame([
            'FIELD_DELIMITER = \',\'',
            'SKIP_HEADER = 1',
            'ESCAPE_UNENCLOSED_FIELD = \'\\\\\'',
        ], $result);
    }
}
