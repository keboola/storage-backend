<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Exasol\ToStage;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
use Keboola\Db\ImportExport\Backend\Exasol\ToStage\FromS3CopyIntoAdapter;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;
use PHPUnit\Framework\MockObject\MockObject;
use Tests\Keboola\Db\ImportExportUnit\Backend\Exasol\MockConnectionTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class FromS3CopyIntoAdapterTest extends BaseTestCase
{
    use MockConnectionTrait;

    public function testGetCopyCommands(): void
    {
        /** @var Storage\S3\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\S3\SourceFile::class);
        $source->expects(self::any())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects(self::once())->method('getManifestEntries')->willReturn(['https://url']);

        $conn = $this->mockConnection();
        $conn->expects(self::once())->method('executeStatement')->with(
            <<<EOT

IMPORT INTO "schema"."stagingTable" FROM CSV AT ''
USER '' IDENTIFIED BY ''
FILE 'https:url' --- files
--- file_opt

COLUMN SEPARATOR=','
COLUMN DELIMITER='"'

EOT,
        );

        $conn->expects(self::once())->method('fetchOne')
            ->with('SELECT COUNT(*) AS NumberOfRows FROM "schema"."stagingTable"')
            ->willReturn(10);

        $destination = new ExasolTableDefinition(
            'schema',
            'stagingTable',
            true,
            new ColumnCollection([]),
            [],
        );
        $options = new ExasolImportOptions();
        $adapter = new FromS3CopyIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
        );

        self::assertEquals(10, $count);
    }

    public function testGetCopyCommandsRowSkip(): void
    {
        /** @var Storage\S3\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\S3\SourceFile::class);
        $source->expects(self::any())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects(self::once())->method('getManifestEntries')->willReturn(['https://url']);

        $conn = $this->mockConnection();
        $conn->expects(self::once())->method('executeStatement')->with(
            <<<EOT

IMPORT INTO "schema"."stagingTable" FROM CSV AT ''
USER '' IDENTIFIED BY ''
FILE 'https:url' --- files
--- file_opt
SKIP=3
COLUMN SEPARATOR=','
COLUMN DELIMITER='"'

EOT,
        );

        $conn->expects(self::once())->method('fetchOne')
            ->with('SELECT COUNT(*) AS NumberOfRows FROM "schema"."stagingTable"')
            ->willReturn(7);

        $destination = new ExasolTableDefinition(
            'schema',
            'stagingTable',
            true,
            new ColumnCollection([]),
            [],
        );
        $options = new ExasolImportOptions([], false, false, 3);
        $adapter = new FromS3CopyIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
        );

        self::assertEquals(7, $count);
    }

    public function testGetCopyCommandWithMoreChunksOfFiles(): void
    {
        $entries = [];

        for ($i = 1; $i < 40; $i++) {
            $entries[] = "https://url{$i}";
        }

        /** @var Storage\S3\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\S3\SourceFile::class);
        $source->expects(self::any())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects(self::once())->method('getManifestEntries')->willReturn($entries);
        $source->expects(self::once())->method('getS3Prefix')->willReturn('s3://bucket');

        $conn = $this->mockConnection();

        $qTemplate = <<<EOT

IMPORT INTO "schema"."stagingTable" FROM CSV AT ''
USER '' IDENTIFIED BY ''
%s --- files
--- file_opt

COLUMN SEPARATOR=','
COLUMN DELIMITER='"'

EOT;
        $q1 = sprintf($qTemplate, implode("\n", array_map(static function ($file) {
            return "FILE '{$file}'";
        }, array_slice($entries, 0, 32))));
        $q2 = sprintf($qTemplate, implode("\n", array_map(static function ($file) {
            return "FILE '{$file}'";
        }, array_slice($entries, 32, 8))));
        $conn->expects(self::exactly(2))->method('executeStatement')->withConsecutive([$q1], [$q2]);

        $conn->expects(self::once())->method('fetchOne')
            ->with('SELECT COUNT(*) AS NumberOfRows FROM "schema"."stagingTable"')
            ->willReturn(7);

        $destination = new ExasolTableDefinition(
            'schema',
            'stagingTable',
            true,
            new ColumnCollection([]),
            [],
        );
        $options = new ExasolImportOptions();
        $adapter = new FromS3CopyIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
        );

        self::assertEquals(7, $count);
    }
}
