<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportCommon;

use Aws\S3\S3Client;
use DateTimeInterface;
use Exception;
use Flow\Parquet\Reader;
use Google\Cloud\Storage\StorageClient;
use Google\Cloud\Storage\StorageObject;
use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\ABS;
use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\Db\ImportExport\Storage\S3;
use Keboola\FileStorage\Abs\ClientFactory;
use Keboola\Temp\Temp;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\Blob;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use Symfony\Component\Process\Exception\ProcessFailedException;
use Symfony\Component\Process\Process;

trait StorageTrait
{
    use ABSSourceTrait;
    use S3SourceTrait;
    use GCSSourceTrait;
    use ImportTrait;
    use ExportTrait;

    protected function getBuildPrefix(): string
    {
        $buildPrefix = '';
        if (getenv('BUILD_PREFIX') !== false) {
            $buildPrefix = getenv('BUILD_PREFIX');
        }
        return $buildPrefix;
    }

    protected function getExportBlobDir(): string
    {
        $path = '';
        switch (getenv('STORAGE_TYPE')) {
            case StorageType::STORAGE_S3:
                $key = getenv('AWS_S3_KEY');
                if ($key) {
                    $path = $key . '/';
                }
        }

        return $path . 'test_export';
    }

    protected function getDestinationInstance(
        string $filePath,
    ): DestinationInterface {
        switch (getenv('STORAGE_TYPE')) {
            case StorageType::STORAGE_S3:
                return new Storage\S3\DestinationFile(
                    (string) getenv('AWS_ACCESS_KEY_ID'),
                    (string) getenv('AWS_SECRET_ACCESS_KEY'),
                    (string) getenv('AWS_REGION'),
                    (string) getenv('AWS_S3_BUCKET'),
                    $filePath,
                );
            case StorageType::STORAGE_ABS:
                return new Storage\ABS\DestinationFile(
                    (string) getenv('ABS_CONTAINER_NAME'),
                    $filePath,
                    $this->getCredentialsForAzureContainer((string) getenv('ABS_CONTAINER_NAME'), 'rwla'),
                    (string) getenv('ABS_ACCOUNT_NAME'),
                    (string) getenv('ABS_ACCOUNT_KEY'),
                );
            case StorageType::STORAGE_GCS:
                return new Storage\GCS\DestinationFile(
                    (string) getenv($this->getGCSBucketEnvName()),
                    $filePath,
                    (string) getenv('GCS_INTEGRATION_NAME'),
                    $this->getGCSCredentials(),
                );
            default:
                throw new Exception(sprintf('Unknown STORAGE_TYPE "%s".', getenv('STORAGE_TYPE')));
        }
    }

    /**
     * @param string[] $columns
     * @param string[]|null $primaryKeys
     * @return S3\SourceFile|ABS\SourceFile|S3\SourceDirectory|ABS\SourceDirectory
     */
    public function getSourceInstance(
        string $filePath,
        array $columns = [],
        bool $isSliced = false,
        bool $isDirectory = false,
        ?array $primaryKeys = null,
    ) {
        switch (getenv('STORAGE_TYPE')) {
            case StorageType::STORAGE_S3:
                $getSourceInstance = 'createS3SourceInstance';
                $manifestPrefix = 'S3.';
                break;
            case StorageType::STORAGE_ABS:
                $getSourceInstance = 'createABSSourceInstance';
                $manifestPrefix = 'ABS.';
                break;
            case StorageType::STORAGE_GCS:
                $getSourceInstance = 'createGCSSourceInstance';
                $manifestPrefix = 'GCS.';
                if ($isDirectory) {
                    self::markTestSkipped('GCS does not support directory import');
                }
                break;
            default:
                throw new Exception(sprintf('Unknown STORAGE_TYPE "%s".', getenv('STORAGE_TYPE')));
        }

        $filePath = str_replace('%MANIFEST_PREFIX%', $manifestPrefix, $filePath);
        return $this->$getSourceInstance(
            $filePath,
            $columns,
            $isSliced,
            $isDirectory,
            $primaryKeys,
        );
    }

    /**
     * @param string[] $columns
     * @param string[]|null $primaryKeys
     * @return S3\SourceFile|ABS\SourceFile|S3\SourceDirectory|ABS\SourceDirectory
     */
    public function getSourceInstanceFromCsv(
        string $filePath,
        CsvOptions $options,
        array $columns = [],
        bool $isSliced = false,
        bool $isDirectory = false,
        ?array $primaryKeys = null,
    ) {
        switch (getenv('STORAGE_TYPE')) {
            case StorageType::STORAGE_S3:
                $getSourceInstanceFromCsv = 'createS3SourceInstanceFromCsv';
                $manifestPrefix = 'S3.';
                break;
            case StorageType::STORAGE_ABS:
                $getSourceInstanceFromCsv = 'createABSSourceInstanceFromCsv';
                $manifestPrefix = '';
                break;
            case StorageType::STORAGE_GCS:
                $getSourceInstanceFromCsv = 'createGCSSourceInstanceFromCsv';
                $manifestPrefix = '';
                break;
            default:
                throw new Exception(sprintf('Unknown STORAGE_TYPE "%s".', getenv('STORAGE_TYPE')));
        }

        $filePath = str_replace('%MANIFEST_PREFIX%', $manifestPrefix, $filePath);
        return $this->$getSourceInstanceFromCsv(
            $filePath,
            $options,
            $columns,
            $isSliced,
            $isDirectory,
            $primaryKeys,
        );
    }

    public function clearDestination(string $dirToClear): void
    {
        switch (getenv('STORAGE_TYPE')) {
            case StorageType::STORAGE_S3:
                /** @var S3Client $client */
                $client = $this->createClient();
                $client->deleteMatchingObjects(
                    (string) getenv('AWS_S3_BUCKET'),
                    $dirToClear,
                );
                return;
            case StorageType::STORAGE_ABS:
                /** @var BlobRestProxy $client */
                $client = $this->createClient();
                // delete blobs from EXPORT_BLOB_DIR
                $listOptions = new ListBlobsOptions();
                $listOptions->setPrefix($dirToClear);
                $containerName = (string) getenv('ABS_CONTAINER_NAME');
                $blobs = $client->listBlobs($containerName, $listOptions);
                foreach ($blobs->getBlobs() as $blob) {
                    $client->deleteBlob($containerName, $blob->getName());
                }
                return;
            case StorageType::STORAGE_GCS:
                /** @var StorageClient $client */
                $client = $this->createClient();
                $bucket = $client->bucket((string) getenv($this->getGCSBucketEnvName()));
                $objects = $bucket->objects(['prefix' => $dirToClear]);
                foreach ($objects as $object) {
                    $object->delete();
                }
                return;
            default:
                throw new Exception(sprintf('Unknown STORAGE_TYPE "%s".', getenv('STORAGE_TYPE')));
        }
    }

    /**
     * @return S3Client|BlobRestProxy|StorageClient
     */
    public function createClient()
    {
        switch (getenv('STORAGE_TYPE')) {
            case StorageType::STORAGE_S3:
                return new S3Client([
                    'credentials' => [
                        'key' => (string) getenv('AWS_ACCESS_KEY_ID'),
                        'secret' => (string) getenv('AWS_SECRET_ACCESS_KEY'),
                    ],
                    'region' => (string) getenv('AWS_REGION'),
                    'version' => '2006-03-01',
                ]);
            case StorageType::STORAGE_ABS:
                $connectionString = sprintf(
                    'DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net',
                    (string) getenv('ABS_ACCOUNT_NAME'),
                    (string) getenv('ABS_ACCOUNT_KEY'),
                );
                return ClientFactory::createClientFromConnectionString(
                    $connectionString,
                );
            case StorageType::STORAGE_GCS:
                return new StorageClient([
                    'keyFile' => $this->getGCSCredentials(),
                    'debug' => true,
                ]);
            default:
                throw new Exception(sprintf('Unknown STORAGE_TYPE "%s".', getenv('STORAGE_TYPE')));
        }
    }

    /**
     * @return string[]
     */
    public function getFileNames(string $dir, bool $excludeManifest = true): array
    {
        $files = $this->listFiles($dir, $excludeManifest);
        if ($files[0] instanceof Blob) {
            /** @var Blob[] $files */
            return array_map(static fn(Blob $blob): string => $blob->getName(), $files);
        }
        if ($files[0] instanceof StorageObject) {
            /** @var StorageObject[] $files */
            return array_map(static fn(StorageObject $blob): string => $blob->name(), $files);
        }
        if (is_array($files[0]) && array_key_exists('Key', $files[0])) {
            /** @var array<array{Key:string}> $files */
            return array_map(static fn(array $blob): string => $blob['Key'], $files);
        }
        throw new Exception(sprintf('Unknown STORAGE_TYPE "%s".', getenv('STORAGE_TYPE')));
    }

    /**
     * @return Blob[]|array<array{Key:string}>|StorageObject[]
     */
    public function listFiles(string $dir, bool $excludeManifest = true): array
    {
        switch (getenv('STORAGE_TYPE')) {
            case StorageType::STORAGE_S3:
                /** @var S3Client $client */
                $client = $this->createClient();
                $result = $client->listObjects([
                    'Bucket' => (string) getenv('AWS_S3_BUCKET'),
                    'Prefix' => $dir,
                ]);
                /** @var array<array{Key:string}> $blobs */
                $blobs = $result->get('Contents');
                if ($excludeManifest) {
                    $blobs = array_filter(
                        $blobs,
                        static fn(array $blob) => !strpos($blob['Key'], 'manifest'),
                    );
                }
                return $blobs;
            case StorageType::STORAGE_ABS:
                /** @var BlobRestProxy $client */
                $client = $this->createClient();
                $listOptions = new ListBlobsOptions();
                $listOptions->setPrefix($dir);
                $blobs = $client->listBlobs((string) getenv('ABS_CONTAINER_NAME'), $listOptions)->getBlobs();
                if ($excludeManifest) {
                    $blobs = array_filter(
                        $blobs,
                        static fn(Blob $blob) => !strpos($blob->getName(), 'manifest'),
                    );
                }
                return $blobs;
            case StorageType::STORAGE_GCS:
                /** @var StorageClient $client */
                $client = $this->createClient();
                $bucket = $client->bucket((string) getenv($this->getGCSBucketEnvName()));
                $objects = $bucket->objects(['prefix' => $dir]);
                $objects = iterator_to_array($objects);
                if ($excludeManifest) {
                    $objects = array_filter(
                        $objects,
                        static fn(StorageObject $blob) => !strpos($blob->name(), 'manifest'),
                    );
                }
                return $objects;
            default:
                throw new Exception(sprintf('Unknown STORAGE_TYPE "%s".', getenv('STORAGE_TYPE')));
        }
    }

    /**
     * @param Blob[]|array<string[]>|StorageObject[] $files
     * @return CsvFile<string[]>
     */
    public function getCsvFileFromStorage(
        array $files,
        string $tmpName = 'tmp.csv',
    ): CsvFile {
        $tmp = new Temp();
        $tmpFolder = $tmp->getTmpFolder();
        $finalFile = $tmpFolder . $tmpName;
        $tmpFiles = $this->downloadFiles($files, $tmpFolder);
        $this->concatCsv($tmpFiles, $finalFile);
        return new CsvFile($finalFile);
    }

    /**
     * @param Blob[]|array<string[]>|StorageObject[] $files
     * @return string[]
     */
    public function getParquetFileFromStorage(
        array $files,
    ): array {
        $tmp = new Temp();
        $tmpFolder = $tmp->getTmpFolder();
        return $this->downloadFiles($files, $tmpFolder);
    }

    /**
     * @param string[] $tmpFiles
     */
    private function concatCsv(array $tmpFiles, string $finalFile): void
    {
        foreach ($tmpFiles as $file) {
            $process = Process::fromShellCommandline('cat "${:FILE}" >> "${:FINAL_FILE}"');
            $process->setTimeout(null);
            $code = $process->run(null, [
                'FILE' => $file,
                'FINAL_FILE' => $finalFile,
            ]);

            if ($code !== 0) {
                throw new ProcessFailedException($process);
            }
        }
    }

    private function getAbsBlobContent(
        string $blob,
    ): string {
        $client = $this->createClient();
        assert($client instanceof BlobRestProxy);
        $stream = $client
            ->getBlob((string) getenv('ABS_CONTAINER_NAME'), $blob)
            ->getContentStream();

        $content = stream_get_contents($stream);
        if ($content === false) {
            throw new Exception();
        }
        return $content;
    }

    /**
     * @param Blob[]|array<string[]>|StorageObject[] $files
     * @return string[]
     */
    private function downloadFiles(array $files, string $tmpFolder): array
    {
        $tmpFiles = [];
        switch (getenv('STORAGE_TYPE')) {
            case StorageType::STORAGE_S3:
                /** @var S3Client $client */
                $client = $this->createClient();
                /** @var array{Key:string, Body:string} $file */
                foreach ($files as $file) {
                    $result = $client->getObject([
                        'Bucket' => (string) getenv('AWS_S3_BUCKET'),
                        'Key' => $file['Key'],
                    ]);
                    $tmpFiles[] = $tmpName = $tmpFolder . '/' . basename($file['Key']);
                    file_put_contents($tmpName, $result['Body']);
                }
                break;
            case StorageType::STORAGE_ABS:
                foreach ($files as $file) {
                    assert($file instanceof Blob);
                    $content = $this->getAbsBlobContent($file->getName());
                    $tmpFiles[] = $tmpName = $tmpFolder . '/' . basename($file->getName());
                    file_put_contents($tmpName, $content);
                }
                break;
            case StorageType::STORAGE_GCS:
                /** @var StorageClient $client */
                $client = $this->createClient();
                $bucket = $client->bucket((string) getenv($this->getGCSBucketEnvName()));
                foreach ($files as $file) {
                    assert($file instanceof StorageObject);
                    $tmpFiles[] = $tmpName = $tmpFolder . '/' . basename($file->name());
                    $bucket->object($file->name())->downloadToFile($tmpName);
                }
                break;
            default:
                throw new Exception(sprintf('Unknown STORAGE_TYPE "%s".', getenv('STORAGE_TYPE')));
        }
        return $tmpFiles;
    }

    /**
     * @param string[] $files
     * @return array<int<0, max>, array<string, mixed>>
     */
    public function getParquetContent(array $files): array
    {
        $content = [];
        $reader = new Reader();
        foreach ($files as $tmpFile) {
            $file = $reader->read($tmpFile);
            foreach ($file->values() as $row) {
                foreach ($row as $column => &$value) {
                    if ($value instanceof DateTimeInterface) {
                        $row[$column] = $value->format(DateTimeInterface::ATOM);
                    } else {
                        $row[$column] = $value;
                    }
                }
                $content[] = $row;
            }
        }
        return $content;
    }
}
