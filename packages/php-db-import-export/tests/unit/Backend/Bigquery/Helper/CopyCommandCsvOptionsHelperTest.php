<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Bigquery\Helper;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Bigquery\Helper\CopyCommandCsvOptionsHelper;
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
            'field_delimiter=\',\'',
            'quote=\'\"\'',
            'allow_quoted_newlines=true',
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
            'field_delimiter=\',\'',
            'skip_leading_rows=1',
        ], $result);
    }

    public function testGetCsvCopyCommandOptionsSkipHeader(): void
    {
        $result = CopyCommandCsvOptionsHelper::getCsvCopyCommandOptions(
            new ImportOptions([], false, false, 1),
            new CsvOptions(),
        );

        self::assertSame([
            'field_delimiter=\',\'',
            'skip_leading_rows=1',
            'quote=\'\"\'',
            'allow_quoted_newlines=true',
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
            'field_delimiter=\',\'',
            'skip_leading_rows=1',
        ], $result);
    }
}
