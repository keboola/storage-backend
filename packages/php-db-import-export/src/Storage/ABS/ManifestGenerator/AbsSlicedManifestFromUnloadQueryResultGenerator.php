<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS\ManifestGenerator;

use JsonException;
use Keboola\Db\ImportExport\Storage\SlicedManifestGeneratorInterface;
use Keboola\FileStorage\Abs\Path\AbsAbsolutePath;
use Keboola\FileStorage\Path\RelativePath;
use Keboola\FileStorage\Path\RelativePathInterface;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;

/**
 * Generates manifest file from list of files originated in snowflake unload statement
 */
class AbsSlicedManifestFromUnloadQueryResultGenerator implements SlicedManifestGeneratorInterface
{
    private BlobRestProxy $absClient;

    private string $accountName;

    public function __construct(
        BlobRestProxy $absClient,
        string $accountName
    ) {
        $this->absClient = $absClient;
        $this->accountName = $accountName;
    }

    /**
     * @param array<array{FILE_NAME: string, FILE_SIZE: string, ROW_COUNT: string}> $listOfUnloadedFiles
     * @throws JsonException
     */
    public function generateAndSaveManifest(RelativePathInterface $path, array $listOfUnloadedFiles = []): void
    {
        $entries = [];
        foreach ($listOfUnloadedFiles as $object) {
            $entries[] = [
                'url' => (AbsAbsolutePath::createFromRelativePath(
                    RelativePath::create(
                        $path->getProvider(),
                        $path->getRoot(),
                        '',
                        $object['FILE_NAME']
                    ),
                    $this->accountName
                ))->getAbsoluteUrl(),
                'mandatory' => true,
            ];
        }

        $manifest = [
            'entries' => $entries,
        ];
        /** @var string $encodedManifest */
        $encodedManifest = json_encode($manifest, JSON_THROW_ON_ERROR);

        $this->absClient->createBlockBlob(
            $path->getRoot(),
            $path->getPathnameWithoutRoot() . 'manifest',
            $encodedManifest
        );
    }
}
