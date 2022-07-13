<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportCommon\StubLoader;

use Exception;
use Google\Cloud\Storage\StorageClient;
use Google\Cloud\Storage\StorageObject;
use React\Promise\Promise;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;
use Throwable;
use function React\Async\parallel;

class GCSLoader extends BaseStubLoader
{
    private const MANIFEST_SUFFIX = 'GCS';

    private StorageClient $client;

    private string $bucketName;

    /**
     * @param array{
     * type: string,
     * project_id: string,
     * private_key_id: string,
     * private_key: string,
     * client_email: string,
     * client_id: string,
     * auth_uri: string,
     * token_uri: string,
     * auth_provider_x509_cert_url: string,
     * client_x509_cert_url: string,
     * } $credentials
     */
    public function __construct(
        array $credentials,
        string $bucketName
    ) {
        $this->client = new StorageClient([
            'keyFile' => $credentials,
            'debug' => true,
        ]);
        $this->bucketName = $bucketName;
        str_replace('gs://', '', $this->bucketName);
    }

    public function clearBucket(): void
    {
        echo "Clear bucket \n";
        $bucket = $this->client->bucket($this->bucketName);

        $promises = [];
        foreach ($bucket->objects() as $object) {
            $promises[] = fn() => new Promise(function ($resolve) use ($object) {
                $object->delete();
                echo 'Blob deleted: ' . $object->info()['name'] . PHP_EOL;
                return $resolve();
            });
        }

        parallel($promises)->then(function (): void {
            return;
        }, function (Throwable $e): void {
            echo 'Error: ' . $e->getMessage() . PHP_EOL;
        });
    }

    public function load(): void
    {
        $bucket = $this->client->bucket($this->bucketName);
        if (!$bucket->exists()) {
            $this->client->createBucket($this->bucketName);
        }

        $this->generateLargeSliced();
        $this->generateLongCol();
        $this->generateManifests();

        echo "Creating blobs ...\n";

        $files = (new Finder())->in(self::BASE_DIR)->files();
        $promises = [];
        /** @var \Symfony\Component\Finder\SplFileInfo $file */
        foreach ($files as $file) {
            $promises[] = fn() => new Promise(function ($resolve) use ($bucket, $file) {
                $blobName = strtr($file->getPathname(), [self::BASE_DIR => '']);
                $res = $bucket->upload(
                    $file->getContents(),
                    [
                        'name' => $blobName,
                    ]
                );
                echo 'Blob uploaded: ' . $blobName . PHP_EOL;
                return $resolve([$blobName, $res]);
            });
        }

        $promises[] = fn() => new Promise(function ($resolve) use ($bucket) {
            $blobName = '02_tw_accounts.csv.invalid.manifest';
            $res = $bucket->upload(
                json_encode([
                    'entries' => [
                        [
                            'url' => sprintf(
                                'gs://%s/not-exists.csv',
                                $this->bucketName
                            ),
                            'mandatory' => true,
                        ],
                    ],
                ], JSON_THROW_ON_ERROR),
                [
                    'name' => $blobName,
                ]
            );
            return $resolve([$blobName, $res]);
        });

        parallel($promises)->then(function (): void {
            return;
        }, function (Throwable $e): void {
            echo 'Error: ' . $e->getMessage() . PHP_EOL;
        });

        echo "GCS load complete \n";
    }

    private function generateManifests(): void
    {
        echo "Generating manifests ...\n";
        // GENERATE SLICED FILE MANIFEST
        $finder = new Finder();

        $directories = $finder->in(self::BASE_DIR . 'sliced')->directories()->depth(0);

        /** @var SplFileInfo $directory */
        foreach ($directories as $directory) {
            $finder = new Finder();
            $files = $finder->in($directory->getPathname())->files()->depth(0)->name('/^((?!.csvmanifest).)*$/');
            $manifest = ['entries' => []];
            /** @var SplFileInfo $file */
            foreach ($files as $file) {
                $manifest['entries'][] = [
                    'url' => sprintf(
                        'gs://%s/sliced/%s/%s',
                        $this->bucketName,
                        $directory->getBasename(),
                        $file->getFilename()
                    ),
                    'mandatory' => true,
                ];
            }

            $manifestFilePath = sprintf(
                '%s/%s.%s.csvmanifest',
                $directory->getPathname(),
                self::MANIFEST_SUFFIX,
                $directory->getBasename()
            );
            file_put_contents($manifestFilePath, json_encode($manifest, JSON_THROW_ON_ERROR));
        }

        echo "Generating manifests complete...\n";
    }
}
