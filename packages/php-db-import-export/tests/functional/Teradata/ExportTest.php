<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Teradata;

use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Teradata\Exporter;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\S3;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;

class ExportTest extends TeradataBaseTestCase
{
    private const EXPORT_DIR = 'teradata_test_export';

    public function setUp(): void
    {
        parent::setUp();

        $this->clearDestination($this->getExportDir());

        $this->cleanDatabase($this->getDestinationDbName());
        $this->createDatabase($this->getDestinationDbName());

        $this->cleanDatabase($this->getSourceDbName());
        $this->createDatabase($this->getSourceDbName());
    }

    private function getExportDir(): string
    {
        $buildPrefix = '';
        if (getenv('BUILD_PREFIX') !== false) {
            $buildPrefix = getenv('BUILD_PREFIX');
        }

        return self::EXPORT_DIR
            . '-'
            . $buildPrefix
            . '-'
            . getenv('SUITE');
    }

    public function tearDown(): void
    {
        parent::tearDown();
        unset($this->client);
    }

    public function testExportGzip(): void
    {
        // import
        $schema = $this->getDestinationDbName();
        $this->initTable(self::BIGGER_TABLE);
        $file = new CsvFile(self::DATA_DIR . 'big_table.csv');
        $source = $this->getSourceInstance('big_table.csv', $file->getHeader());
        $destination = new Storage\Teradata\Table(
            $schema,
            self::BIGGER_TABLE
        );
        $options = $this->getSimpleImportOptions(ImportOptions::SKIP_FIRST_LINE, false);

        $this->importTable($source, $destination, $options);

        // export
        $source = $destination;
        $options = $this->getExportOptions(false);
        $destination = $this->getDestinationInstance($this->getExportDir() . '/gz_test/gzip.csv');

        (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $options
        );

        $files = $this->listFiles($this->getExportDir());
        self::assertNotNull($files);
        self::assertCount(10, $files);
    }


    /**
     * @param Storage\Teradata\Table $destination
     * @param S3\SourceFile|S3\SourceDirectory $source
     * @param TeradataImportOptions $options
     * @throws \Doctrine\DBAL\Exception
     */
    private function importTable(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destinationTable,
        ImportOptions $options
    ): void {
        $importer = new ToStageImporter($this->connection);
        $destinationRef = new TeradataTableReflection(
            $this->connection,
            $destinationTable->getSchema(),
            $destinationTable->getTableName()
        );
        /** @var TeradataTableDefinition $destination */
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $destination,
            $source->getColumnsNames()
        );
        $qb = new TeradataTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );
        $toFinalTableImporter = new FullImporter($this->connection);
        $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState
        );

    }

    public function testExportSimple(): void
    {
        // import
        $this->initTable(self::TABLE_OUT_CSV_2COLS);
        $file = new CsvFile(self::DATA_DIR . 'with-ts.csv');
        $source = $this->getSourceInstance('with-ts.csv', $file->getHeader());
        $destination = new Storage\Teradata\Table(
            $this->getDestinationDbName(),
            'out_csv_2Cols'
        );
        $options = $this->getSimpleImportOptions();
        $this->importTable($source, $destination, $options);

        // export
        $source = $destination;
        $options = $this->getExportOptions();
        $destination = $this->getDestinationInstance($this->getExportDir() . '/ts_test/ts_test');

        (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $options
        );

        $files = $this->listFiles($this->getExportDir());
        self::assertNotNull($files);

        $actual = $this->getCsvFileFromStorage($files);
        $expected = new CsvFile(
            self::DATA_DIR . 'with-ts.expected.exasol.csv',
            CsvOptions::DEFAULT_DELIMITER,
            CsvOptions::DEFAULT_ENCLOSURE,
            CsvOptions::DEFAULT_ESCAPED_BY,
            1 // skip header
        );
        $this->assertCsvFilesSame($expected, $actual);

    }

    public function testExportSimpleWithQuery(): void
    {
        // TODO
    }
}
