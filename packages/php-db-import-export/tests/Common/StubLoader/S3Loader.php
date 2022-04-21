<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportCommon\StubLoader;

use Aws\S3\S3Client;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;
use function \GuzzleHttp\json_encode as guzzle_json_encode;

class S3Loader extends BaseStubLoader
{
    private const MANIFEST_SUFFIX = 'S3';

    private string $bucket;

    private S3Client $client;

    private string $key;

    private string $path;

    public function __construct(
        string $region,
        string $bucket,
        string $key
    ) {
        $this->client = new S3Client([
            'region' => $region,
            'version' => '2006-03-01',
        ]);

        $this->bucket = $bucket;
        $this->key = $key;
        $this->path = $this->bucket . '/' . $this->key . '/';
    }

    public function clearBucket(): void
    {
        $result = $this->client->listObjects([
            'Bucket' => $this->bucket,
            'Prefix' => $this->key,
            'Delimiter' => '/',
        ]);
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
        $this->generateLargeSliced();
        $this->generateManifests();

        echo "Creating blobs ...\n";
        //UPLOAD ALL FILES TO S3
        // Create a transfer object.
        $manager = new \Aws\S3\Transfer(
            $this->client,
            self::BASE_DIR,
            's3://' . $this->path,
            [
                'debug' => true,
            ]
        );

        // Perform the transfer synchronously.
        $manager->transfer();

        $this->client->putObject([
            'Bucket' => $this->bucket,
            'Key' => '02_tw_accounts.csv.invalid.manifest',
            'Body' => json_encode([
                'entries' => [
                    [
                        'url' => sprintf(
                            's3://%snot-exists.csv',
                            $this->path
                        ),
                        'mandatory' => true,
                    ],
                ],
            ]),
        ]);

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
                        's3://%ssliced/%s/%s',
                        $this->path,
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
            file_put_contents($manifestFilePath, guzzle_json_encode($manifest));
        }

        echo "Generating manifests complete...\n";
    }
}
