<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\TestsStubLoader;

use Aws\S3\S3Client;
use Aws\S3\Transfer;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;

class S3Loader extends BaseStubLoader
{
    private const MANIFEST_SUFFIX = 'S3';

    private string $bucket;

    private S3Client $client;

    public function __construct(
        string $region,
        string $bucket,
    ) {
        $this->client = new S3Client([
            'region' => $region,
            'version' => '2006-03-01',
        ]);

        $this->bucket = $bucket;
    }

    public function clearBucket(): void
    {
        $result = $this->client->listObjects([
            'Bucket' => $this->bucket,
            'Delimiter' => '/',
        ]);
        /** @var array<array{Key: string}>|null $objects */
        $objects = $result->get('Contents');
        if ($objects) {
            $this->client->deleteObjects([
                'Bucket' => $this->bucket,
                'Delete' => [
                    'Objects' => array_map(static function ($object) {
                        return [
                            'Key' => $object['Key'],
                        ];
                    }, $objects),
                ],
            ]);
        }
    }

    public function load(): void
    {
        $this->generateManifests();
        echo "Creating blobs ...\n";
        //UPLOAD ALL FILES TO S3
        // Create a transfer object.
        $manager = new Transfer(
            $this->client,
            self::BASE_DIR,
            's3://' . $this->bucket,
            [
                'debug' => true,
            ],
        );

        // Perform the transfer synchronously.
        $manager->transfer();
        echo "S3 load complete \n";
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
                        's3://%s/sliced/%s/%s',
                        $this->bucket,
                        $directory->getBasename(),
                        $file->getFilename(),
                    ),
                    'mandatory' => true,
                ];
            }

            $manifestFilePath = sprintf(
                '%s/%s.%s.csvmanifest',
                $directory->getPathname(),
                self::MANIFEST_SUFFIX,
                $directory->getBasename(),
            );
            file_put_contents($manifestFilePath, json_encode($manifest, JSON_THROW_ON_ERROR));
        }

        echo "Generating manifests complete...\n";
    }
}
