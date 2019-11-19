<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Backend\Snowflake\Exporter;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\ColumnsHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Temp\Temp;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;

class ExportTest extends SnowflakeImportExportBaseTest
{
    private const EXPORT_BLOB_DIR = 'test_export';

    /**
     * @var BlobRestProxy
     */
    private $blobClient;

    public function setUp(): void
    {
        parent::setUp();
        $connectionString = sprintf(
            'DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net',
            (string) getenv('ABS_ACCOUNT_NAME'),
            (string) getenv('ABS_ACCOUNT_KEY')
        );
        $this->blobClient = BlobRestProxy::createBlobService($connectionString);
        // delete blobs from EXPORT_BLOB_DIR
        $listOptions = new ListBlobsOptions();
        $listOptions->setPrefix(self::EXPORT_BLOB_DIR);
        $blobs = $this->blobClient->listBlobs((string) getenv('ABS_CONTAINER_NAME'), $listOptions);
        foreach ($blobs->getBlobs() as $blob) {
            $this->blobClient->deleteBlob((string) getenv('ABS_CONTAINER_NAME'), $blob->getName());
        }
    }

    public function tearDown(): void
    {
        parent::tearDown();
        unset($this->blobClient);
    }

    public function testExportGzip(): void
    {
        // import
        $file = new CsvFile(self::DATA_DIR . 'with-ts.csv');
        $source = $this->createABSSourceInstance('with-ts.csv');
        $destination = new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'out.csv_2Cols');
        $options = $this->getSimpleImportOptions($file->getHeader());

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        // export
        $source = $destination;
        $options = new ExportOptions(null, true);
        $destination = $this->createABSSourceDestinationInstance(self::EXPORT_BLOB_DIR . '/gz_test');

        (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $options
        );

        $resource = $this->getBlobResource($destination->getFilePath() . '_0_0_0.csv.gz');
        // this not failing is sign that table was exported successfully
        self::assertIsResource($resource);
    }

    /**
     * @param string $blob
     * @return resource
     */
    private function getBlobResource(string $blob)
    {
        return $this->blobClient
            ->getBlob((string) getenv('ABS_CONTAINER_NAME'), $blob)
            ->getContentStream();
    }

    public function testExportSimple(): void
    {
        // import
        $file = new CsvFile(self::DATA_DIR . 'with-ts.csv');
        $source = $this->createABSSourceInstance('with-ts.csv');
        $destination = new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'out.csv_2Cols');
        $options = $this->getSimpleImportOptions($file->getHeader());

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        // export
        $source = $destination;
        $options = new ExportOptions();
        $destination = $this->createABSSourceDestinationInstance(self::EXPORT_BLOB_DIR . '/ts_test');

        (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $options
        );

        $actual = $this->getCsvFileFromBlob($destination->getFilePath() . '_0_0_0.csv');
        $expected = new CsvFile(
            self::DATA_DIR . 'with-ts.csv',
            CsvOptions::DEFAULT_DELIMITER,
            CsvOptions::DEFAULT_ENCLOSURE,
            CsvOptions::DEFAULT_ESCAPED_BY,
            1 // skip header
        );
        $this->assertCsvFilesSame($expected, $actual);
    }

    private function getCsvFileFromBlob(
        string $filePath,
        string $tmpName = 'tmp.csv'
    ): CsvFile {
        $content = $this->getBlobContent($filePath);
        $tmp = new Temp();
        $tmp->initRunFolder();
        $actual = $tmp->getTmpFolder() . $tmpName;
        file_put_contents($actual, $content);
        return new CsvFile($actual);
    }

    private function getBlobContent(
        string $blob
    ): string {
        $content = stream_get_contents($this->getBlobResource($blob));
        if ($content === false) {
            throw new \Exception();
        }
        return $content;
    }

    public function assertCsvFilesSame(CsvFile $expected, CsvFile $actual): void
    {
        $this->assertArrayEqualsSorted(
            iterator_to_array($expected),
            iterator_to_array($actual),
            0,
            'Csv files are not same'
        );
    }

    public function testExportSimpleWithQuery(): void
    {
        // import
        $file = new CsvFile(self::DATA_DIR . 'tw_accounts.csv');
        $source = $this->createABSSourceInstance('tw_accounts.csv');
        $destination = new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'accounts-3');
        $options = $this->getSimpleImportOptions($file->getHeader());

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        // export
        $source = $destination;
        // query needed otherwise timestamp is downloaded
        $query = sprintf(
            'SELECT %s FROM %s',
            ColumnsHelper::getColumnsString($file->getHeader()),
            $source->getQuotedTableWithScheme()
        );
        $options = new ExportOptions($query);
        $destination = $this->createABSSourceDestinationInstance(self::EXPORT_BLOB_DIR . '/tw_test');

        (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $options
        );

        $actual = $this->getCsvFileFromBlob($destination->getFilePath() . '_0_0_0.csv');
        $expected = new CsvFile(
            self::DATA_DIR . 'tw_accounts.csv',
            CsvOptions::DEFAULT_DELIMITER,
            CsvOptions::DEFAULT_ENCLOSURE,
            CsvOptions::DEFAULT_ESCAPED_BY,
            1 // skip header
        );
        $this->assertCsvFilesSame($expected, $actual);
    }
}
