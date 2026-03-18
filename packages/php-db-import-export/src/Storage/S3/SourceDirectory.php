<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\S3;

class SourceDirectory extends SourceFile
{
    /**
     * returns all files in directory
     *
     * @return string[]
     */
    public function getManifestEntries(): array
    {
        $client = $this->getClient();
        $prefix = $this->getPrefix();
        $response = $client->listObjectsV2([
            'Bucket' => $this->bucket,
            'Delimiter' => '/',
            'Prefix' => $prefix,
            ],);

        /** @var array<int, array{Key: string}> $contents */
        $contents = $response->get('Contents');
        return array_map(
            static function (array $file): string {
                return $file['Key'];
            },
            $contents,
        );
    }

    public function getPrefix(): string
    {
        $prefix = $this->filePath;
        if (substr($prefix, -1) !== '/') {
            // add trailing slash if not set to list only blobs in folder
            $prefix .= '/';
        }

        return $prefix;
    }
}
