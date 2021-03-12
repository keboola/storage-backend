<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\S3;

class SourceDirectory extends SourceFile
{
    /**
     * @return string[]
     */
    public function getManifestEntries(): array
    {
        $client = $this->getClient();
        $prefix = $this->filePath;
        if (substr($prefix, -1) !== '/') {
            // add trailing slash if not set to list only blobs in folder
            $prefix .= '/';
        }
        $response = $client->listObjectsV2([
            'Bucket' => $this->bucket,
            'Delimiter' => '/',
            'Prefix' => $prefix,
        ]);

        return array_map(static function (array $file) {
            return $file['Key'];
        }, $response->get('Contents'));
    }
}
