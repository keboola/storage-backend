<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Teradata;

use Keboola\Db\ImportExport\Backend\Teradata\Helper\StorageS3Helper;
use Keboola\Db\ImportExport\Storage\S3\SourceFile;
use PHPUnit\Framework\TestCase;

class StorageABSHelperTest extends TestCase
{
    /**
     * @param string[] $entriesData
     * @dataProvider dataProvider
     */
    public function testGetResult(string $expected, array $entriesData): void
    {
        $this->markTestSkipped('TODO Not implemented yet');
        /*$mock = $this->createMock(SourceFile::class);

        $mock->method('getManifestEntries')
            ->willReturn($entriesData);
        $mock->method('getS3Prefix')
            ->willReturn('s3://zajca-php-db-import-test-s3filesbucket-bwdj3sk0c9xy');
        $this->assertEquals($expected, StorageS3Helper::getMask($mock));*/
    }

    /**
     * @return array<mixed>
     */
    public function dataProvider(): array
    {
        return [
            'long slice' => [
                'sliced/accounts-gzip/tw_accounts.csv.gz000*_part_00.gz',
                [
                    // phpcs:ignore
                    's3://zajca-php-db-import-test-s3filesbucket-bwdj3sk0c9xy/sliced/accounts-gzip/tw_accounts.csv.gz0001_part_00.gz',
                    // phpcs:ignore
                    's3://zajca-php-db-import-test-s3filesbucket-bwdj3sk0c9xy/sliced/accounts-gzip/tw_accounts.csv.gz0002_part_00.gz',
                ],
            ],
            'suffix style' => [
                'sliced.csv_*',
                [
                    'sliced.csv_1001',
                    'sliced.csv_0',
                    'sliced.csv_1',
                    'sliced.csv_10',
                    'sliced.csv_100',
                    'sliced.csv_1002',
                ],
            ],
            'inner slice' => [
                'sliced0****',
                [
                    'sliced0122.csv',
                    'sliced012.csv',
                    'sliced01.csv',
                    'sliced01999.csv',
                    'sliced0.csv',
                    'sliced034.csv',
                ],
            ],
            'single file' => [
                'singlFile.csv',
                [
                    'singlFile.csv',
                ],
            ],
        ];
    }

    /**
     * @return array<mixed>
     */
    public function isMultipartDataprovider(): array
    {
        return [

            'multipart' => [
                true,
                [
                    's3://zajca-aaaaa/sliced/accounts-gzip/file.gz/F00000',
                ],
            ],

            'single file' => [
                false,
                [
                    's3://zajca-aaaaa/sliced/accounts-gzip/file.gz',
                ],
            ],
        ];
    }

    /**
     * @param string[] $entriesData
     * @dataProvider isMultipartDataprovider
     */
    public function testIsMultipart(bool $expected, array $entriesData): void
    {
        $mock = $this->createMock(SourceFile::class);

        $mock->method('getManifestEntries')
            ->willReturn($entriesData);
        self::assertEquals($expected, StorageS3Helper::isMultipartFile($mock));
    }

    /**
     * @return array<mixed>
     */
    public function buildPrefixProvider(): array
    {
        return [

            'with prefix' => [
                ['sliced/accounts-gzip/', 'file.gz'],
                [
                    's3://zajca-aaaaa/sliced/accounts-gzip/file.gz/F00000',
                ],
            ],

            'without prefix' => [
                ['', 'file.gz'],
                [
                    's3://zajca-aaaaa/file.gz/F00000',
                ],
            ],
        ];
    }

    /**
     * @param string[] $expected
     * @param string[] $entries
     * @dataProvider buildPrefixProvider
     */
    public function testBuildPrefixAndObject(array $expected, array $entries): void
    {
        $this->markTestSkipped('TODO Not implemented yet');
        /*$mock = $this->createMock(SourceFile::class);

        $mock->method('getManifestEntries')
            ->willReturn($entries);
        $mock->method('getS3Prefix')
            ->willReturn('s3://zajca-aaaaa');
        self::assertEquals($expected, StorageS3Helper::buildPrefixAndObject($mock));*/
    }
}
