<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS\ManifestGenerator;

use JsonException;
use Keboola\Db\ImportExport\Storage\ABS\BlobIterator;
use Keboola\Db\ImportExport\Storage\SlicedManifestGeneratorInterface;
use Keboola\FileStorage\Path\RelativePathInterface;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;

/**
 * Generates manifest file by iterating files in a abs path
 */
class AbsSlicedManifestFromFolderGenerator implements SlicedManifestGeneratorInterface
{
    public const GENERATE_MANIFEST_AFTER_POLY_BASE_EXPORT = true;
    public const GENERATE_MANIFEST_DEFAULT = false;

    private BlobRestProxy $absClient;

    private bool $usingPolyBaseExport;

    public function __construct(
        BlobRestProxy $absClient,
        bool $usingPolyBaseExport = self::GENERATE_MANIFEST_DEFAULT
    ) {
        $this->absClient = $absClient;
        $this->usingPolyBaseExport = $usingPolyBaseExport;
    }

    /**
     * @throws JsonException
     */
    public function generateAndSaveManifest(RelativePathInterface $path): void
    {
        $pathToSearchSlices = $path->getPathnameWithoutRoot();
        if ($this->usingPolyBaseExport === self::GENERATE_MANIFEST_AFTER_POLY_BASE_EXPORT) {
            // PolyBase creating empty file with same name as folder with exported csv
            // this is making noise and without separator / it's recorded to manifest
            $pathToSearchSlices .= '/';
        }
        $listOptions = new ListBlobsOptions();
        $listOptions->setPrefix($pathToSearchSlices);

        $iterator = new BlobIterator($this->absClient, $path->getRoot(), $listOptions);

        $entries = [];
        foreach ($iterator as $blob) {
            $url = str_replace('https://', 'azure://', $blob->getUrl());
            $entries[] = [
                'url' => $url,
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
