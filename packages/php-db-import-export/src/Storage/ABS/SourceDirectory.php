<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use MicrosoftAzure\Storage\Blob\Models\Blob;
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
        $result = $blobClient->listBlobs($this->container, $options);
        $entries = array_map(function (Blob $blob) {
            return $blob->getUrl();
        }, $result->getBlobs());

        return $this->transformManifestEntries($entries, $protocol, $blobClient);
    }
}
