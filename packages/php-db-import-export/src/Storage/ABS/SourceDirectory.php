<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Keboola\Db\ImportExport\Storage\FileNotFoundException;
use Keboola\FileStorage\Abs\AbsProvider;
use Keboola\FileStorage\Abs\LineEnding\LineEndingDetector;
use Keboola\FileStorage\FileNotFoundException as FileStorageFileNotFoundException;
use Keboola\FileStorage\LineEnding\StringLineEndingDetectorHelper;
use Keboola\FileStorage\Path\RelativePath;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;

class SourceDirectory extends SourceFile
{
    /**
     * @return StringLineEndingDetectorHelper::EOL_*
     */
    public function getLineEnding(): string
    {
        $client = $this->getBlobClient();
        $detector = LineEndingDetector::createForClient($client);

        $iterator = $this->getEntriesInFolder($client);
        if ($iterator->valid() === false) {
            return StringLineEndingDetectorHelper::EOL_UNIX;
        }
        $blob = $iterator->current();
        $file = RelativePath::createFromRootAndPath(
            new AbsProvider(),
            $this->container,
            $this->getBlobPath($blob->getUrl())
        );

        try {
            return $detector->getLineEnding($file);
        } catch (FileStorageFileNotFoundException $e) {
            throw FileNotFoundException::createFromFileNotFoundException($e);
        }
    }

    private function getEntriesInFolder(BlobRestProxy $blobClient): BlobIterator
    {
        $path = $this->filePath;
        if (substr($path, -1) !== '/') {
            // add trailing slash if not set to list only blobs in folder
            $path .= '/';
        }
        $options = new ListBlobsOptions();
        $options->setPrefix($path);
        return new BlobIterator($blobClient, $this->container, $options);
    }

    /**
     * @return string[]
     */
    public function getManifestEntries(
        string $protocol = self::PROTOCOL_AZURE
    ): array {
        $blobClient = $this->getBlobClient();

        $entries = [];
        foreach ($this->getEntriesInFolder($blobClient) as $blob) {
            $entries[] = $blob->getUrl();
        }

        return $this->transformManifestEntries($entries, $protocol, $blobClient);
    }
}
