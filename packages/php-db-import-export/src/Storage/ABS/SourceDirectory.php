<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;

class SourceDirectory extends SourceFile
{
    public function getManifestEntries(
        string $protocol = self::PROTOCOL_AZURE
    ): array {
        $blobClient = $this->getBlobClient();

        $path = $this->filePath;
        if (substr($path, -1) !== '/') {
            // add trailing slash if not set to list only blobs in folder
            $path .= '/';
        }
        $options = new ListBlobsOptions();
        $options->setPrefix($path);
        $blobIterator = new BlobIterator($blobClient, $this->container, $options);

        $entries = [];
        foreach ($blobIterator as $blob) {
            $entries[] = $blob->getUrl();
        }

        return $this->transformManifestEntries($entries, $protocol, $blobClient);
    }
}
