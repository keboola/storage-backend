<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExport;

use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use MicrosoftAzure\Storage\Common\Internal\Resources;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;
use function \GuzzleHttp\json_encode as guzzle_json_encode;

class AbsLoader
{
    private const BASE_DIR = __DIR__ . '/data/';

    /**
     * @var string
     */
    private $accountName;

    /**
     * @var string
     */
    private $containerName;

    /**
     * @var string
     */
    private $connectionString;

    /**
     * @var BlobRestProxy
     */
    private $blobService;

    public function __construct(
        string $accountName,
        string $containerName
    ) {
        $this->accountName = $accountName;
        $this->containerName = $containerName;
        $this->connectionString = sprintf(
            'DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net',
            $accountName,
            getenv('ABS_ACCOUNT_KEY')
        );
    }

    public function createContainer(): void
    {
        $created = false;
        while ($created === false) {
            try {
                echo "Creating a container \n";
                $this->getBlobService()->createContainer($this->containerName);
                echo "Container created \n";
                $created = true;
            } catch (\MicrosoftAzure\Storage\Common\Exceptions\ServiceException $e) {
                if (preg_match('~The specified container is being deleted.~', $e->getMessage())) {
                    echo "Waiting, because old container is being deleted ... \n";
                    sleep(2);
                } else {
                    throw  $e;
                }
            }
        }
    }

    public function getBlobService(): BlobRestProxy
    {
        if ($this->blobService === null) {
            echo "Creating blob service \n";
            $this->blobService = BlobRestProxy::createBlobService($this->connectionString);
        }
        return $this->blobService;
    }

    public function deleteContainer(): void
    {
        try {
            echo "Deleting a previous container \n";
            $this->getBlobService()->deleteContainer($this->containerName);
            sleep(1);
        } catch (ServiceException $e) {
            if (preg_match('~The specified container does not exist~', $e->getMessage())) {
                echo "Container does not exists. Deleting skipped\n";
            } else {
                throw $e;
            }
        }
    }

    private function generateLongCol(): void
    {
        $file = self::BASE_DIR . 'long_col.csv';
        file_put_contents(
            $file,
            "\"col1\",\"col2\"\n"
        );
        $fp = fopen($file, 'ab');
        fwrite($fp, '"');
        for ($i = 0; $i <= 8000; $i++) {
            fwrite($fp, 'a');
        }
        fwrite($fp, '","b"');
        fclose($fp);
    }

    public function load(): void
    {
        $this->generateLargeSliced();
        $this->generateLongCol();
        $this->generateManifests();

        echo "Creating blobs ...\n";
        //UPLOAD ALL FILES TO ABS
        $files = (new Finder())->in(self::BASE_DIR)->files();

        /** @var \Symfony\Component\Finder\SplFileInfo $file */
        foreach ($files as $file) {
            $this->getBlobService()->createBlockBlob(
                $this->containerName,
                strtr($file->getPathname(), [self::BASE_DIR => '']),
                $file->getContents()
            );
        }

        // invalid manifest
        $this->getBlobService()->createBlockBlob(
            $this->containerName,
            '02_tw_accounts.csv.invalid.manifest',
            json_encode([
                'entries' => [
                    [
                        'url' => sprintf(
                            'azure://%s.%s/%s/not-exists.csv',
                            $this->accountName,
                            Resources::BLOB_BASE_DNS_NAME,
                            $this->containerName
                        ),
                        'mandatory' => true,
                    ],
                ],
            ])
        );

        echo "ABS load complete \n";
    }

    private function generateLargeSliced(): void
    {
        for ($i = 0; $i <= 1500; $i++) {
            $sliceName = sprintf('sliced.csv_%d', $i);
            file_put_contents(
                self::BASE_DIR . 'sliced/2cols-large/' . $sliceName,
                "\"a\",\"b\"\n"
            );
        }
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
                        'azure://%s.%s/%s/sliced/%s/%s',
                        $this->accountName,
                        Resources::BLOB_BASE_DNS_NAME,
                        $this->containerName,
                        $directory->getBasename(),
                        $file->getFilename()
                    ),
                    'mandatory' => true,
                ];
            }

            $manifestFilePath = sprintf(
                '%s/%s.csvmanifest',
                $directory->getPathname(),
                $directory->getBasename()
            );
            file_put_contents($manifestFilePath, guzzle_json_encode($manifest));
        }

        echo "Generating manifests complete...\n";
    }
}
