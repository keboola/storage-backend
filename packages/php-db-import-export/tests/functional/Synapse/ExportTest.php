<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Synapse;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Backend\Synapse\Exporter;
use Keboola\Db\ImportExport\Backend\Synapse\Importer;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseExportOptions;
use Keboola\Temp\Temp;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;

class ExportTest extends SynapseBaseTestCase
{
    private const EXPORT_BLOB_DIR = 'synapse_test_export';

    public function getExportBlobDir(): string
    {
        return self::EXPORT_BLOB_DIR
            . '-'
            . getenv('CREDENTIALS_IMPORT_TYPE')
            . '-'
            . getenv('CREDENTIALS_EXPORT_TYPE');
    }

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
        $listOptions->setPrefix($this->getExportBlobDir());
        $blobs = $this->blobClient->listBlobs((string) getenv('ABS_CONTAINER_NAME'), $listOptions);
        foreach ($blobs->getBlobs() as $blob) {
            $this->blobClient->deleteBlob((string) getenv('ABS_CONTAINER_NAME'), $blob->getName());
        }
        $this->dropAllWithinSchema($this->getDestinationSchemaName());
        $this->dropAllWithinSchema($this->getSourceSchemaName());
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', $this->getDestinationSchemaName()));
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', $this->getSourceSchemaName()));
    }

    public function tearDown(): void
    {
        parent::tearDown();
        unset($this->blobClient);
    }

    public function testExportGzip(): void
    {
        $this->initTables([self::TABLE_OUT_CSV_2COLS]);
        // import
        $file = new CsvFile(self::DATA_DIR . 'with-ts.csv');
        $source = $this->createABSSourceInstance('with-ts.csv', $file->getHeader());
        $destination = new Storage\Synapse\Table($this->getDestinationSchemaName(), 'out.csv_2Cols');
        $options = $this->getSynapseImportOptions();

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        // export
        $source = $destination;
        $options = new SynapseExportOptions(true, getenv('CREDENTIALS_EXPORT_TYPE'));
        $destinationBlob = $this->getExportBlobDir() . '/gz_test/';
        $destination = $this->createABSSourceDestinationInstance($destinationBlob);

        (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $options
        );

        $listOptions = new ListBlobsOptions();
        $listOptions->setPrefix($destinationBlob);
        $blobs = $this->blobClient->listBlobs((string) getenv('ABS_CONTAINER_NAME'), $listOptions);
        foreach ($blobs->getBlobs() as $blob) {
            $this->assertStringEndsWith('.txt.gz', $blob->getName());
            $resource = $this->getBlobResource($blob->getName());
            $this->assertIsResource($resource);
        }
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
        $this->initTables([self::TABLE_OUT_CSV_2COLS]);

        // import
        $file = new CsvFile(self::DATA_DIR . 'with-ts.csv');
        $source = $this->createABSSourceInstance('with-ts.csv', $file->getHeader());
        $destination = new Storage\Synapse\Table($this->getDestinationSchemaName(), 'out.csv_2Cols');
        $options = $this->getSynapseImportOptions();

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        // export
        $source = $destination;
        $options = new SynapseExportOptions(false, getenv('CREDENTIALS_EXPORT_TYPE'));
        $destinationBlob = $this->getExportBlobDir() . '/ts_test/';
        $destination = $this->createABSSourceDestinationInstance($destinationBlob);

        (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $options
        );

        $listOptions = new ListBlobsOptions();
        $listOptions->setPrefix($destinationBlob);
        $blobs = $this->blobClient->listBlobs((string) getenv('ABS_CONTAINER_NAME'), $listOptions);
        $actualContent = [];
        foreach ($blobs->getBlobs() as $blob) {
            $this->assertStringEndsWith('.txt', $blob->getName());
            $resource = $this->getBlobResource($blob->getName());
            $this->assertIsResource($resource);
            $actualContent[] = $this->getCsvFileFromBlob($blob->getName());
        }

        $expected = new CsvFile(
            self::DATA_DIR . 'with-ts.csv',
            CsvOptions::DEFAULT_DELIMITER,
            CsvOptions::DEFAULT_ENCLOSURE,
            CsvOptions::DEFAULT_ESCAPED_BY,
            1 // skip header
        );
        $this->assertCsvFilesCountSliced([$expected], $actualContent);
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

    public function testExportSimpleWithQuery(): void
    {
        $this->initTables([self::TABLE_ACCOUNTS_3]);
        // import
        $file = new CsvFile(self::DATA_DIR . 'tw_accounts.csv');
        $source = $this->createABSSourceInstance('tw_accounts.csv', $file->getHeader());
        $destination = new Storage\Synapse\Table($this->getDestinationSchemaName(), 'accounts-3');
        $options = $this->getSynapseImportOptions();

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );
        $cols = implode(', ', array_map(function ($column) {
            return sprintf(
                'REPLACE(%s, \'"\', \'""\') AS %s',
                $this->platform->quoteSingleIdentifier($column),
                $this->platform->quoteSingleIdentifier($column)
            );
        }, $file->getHeader()));

        // export
        // query needed otherwise timestamp is downloaded
        $query = sprintf(
            'SELECT %s FROM %s',
            $cols,
            $destination->getQuotedTableWithScheme()
        );
        $source = new Storage\Synapse\SelectSource($query);
        $options = new SynapseExportOptions(false, getenv('CREDENTIALS_EXPORT_TYPE'));
        $destinationBlob = $this->getExportBlobDir() . '/tw_test/';
        $destination = $this->createABSSourceDestinationInstance($this->getExportBlobDir() . '/tw_test');

        (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $options
        );

        $listOptions = new ListBlobsOptions();
        $listOptions->setPrefix($destinationBlob);
        $blobs = $this->blobClient->listBlobs((string) getenv('ABS_CONTAINER_NAME'), $listOptions);
        $actualContent = [];
        foreach ($blobs->getBlobs() as $blob) {
            $this->assertStringEndsWith('.txt', $blob->getName());
            $resource = $this->getBlobResource($blob->getName());
            $this->assertIsResource($resource);
            $actualContent[] = $this->getCsvFileFromBlob($blob->getName());
        }

        $expected = new CsvFile(
            self::DATA_DIR . 'tw_accounts.csv',
            CsvOptions::DEFAULT_DELIMITER,
            CsvOptions::DEFAULT_ENCLOSURE,
            CsvOptions::DEFAULT_ESCAPED_BY,
            1 // skip header
        );
        $this->assertCsvFilesSameSliced([$expected], $actualContent);
    }

    public function testExportSimpleWithQueryBindings(): void
    {
        $this->initTables([self::TABLE_ACCOUNTS_3]);
        // import
        $file = new CsvFile(self::DATA_DIR . 'tw_accounts.csv');
        $source = $this->createABSSourceInstance('tw_accounts.csv', $file->getHeader());
        $destination = new Storage\Synapse\Table($this->getDestinationSchemaName(), 'accounts-3');
        $options = $this->getSynapseImportOptions();

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        // export
        // query needed otherwise timestamp is downloaded
        $query = sprintf(
            'SELECT %s FROM %s WHERE [id] > ?',
            $this->qb->getColumnsString($file->getHeader()),
            $destination->getQuotedTableWithScheme()
        );
        $source = new Storage\Synapse\SelectSource($query, [15]);
        $options = new SynapseExportOptions(false, getenv('CREDENTIALS_EXPORT_TYPE'));
        $destinationBlob = $this->getExportBlobDir() . '/tw_test/';
        $destination = $this->createABSSourceDestinationInstance($this->getExportBlobDir() . '/tw_test');

        (new Exporter($this->connection))->exportTable(
            $source,
            $destination,
            $options
        );

        $listOptions = new ListBlobsOptions();
        $listOptions->setPrefix($destinationBlob);
        $blobs = $this->blobClient->listBlobs((string) getenv('ABS_CONTAINER_NAME'), $listOptions);
        $actualContentResponse = [];
        foreach ($blobs->getBlobs() as $blob) {
            $this->assertStringEndsWith('.txt', $blob->getName());
            $resource = $this->getBlobResource($blob->getName());
            $this->assertIsResource($resource);
            $actualContentResponse[] = $this->getCsvFileFromBlob($blob->getName());
        }

        $actualContent = [];
        foreach ($actualContentResponse as $item) {
            $item = iterator_to_array($item);
            if (empty($item)) {
                continue;
            }
            $actualContent = array_merge($actualContent, $item);
        }

        // where limits from 3 to 2
        $this->assertCount(2, $actualContent);
    }
}
