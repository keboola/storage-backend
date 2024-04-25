<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\GCS\ManifestGenerator;

use Google\Cloud\Storage\StorageClient;
use Google\Cloud\Storage\WriteStream;
use JsonException;
use Keboola\Db\ImportExport\Storage\SlicedManifestGeneratorInterface;
use Keboola\FileStorage\Path\RelativePathInterface;

/**
 * Generates manifest file from list of files originated in snowflake unload statement
 */
class GcsSlicedManifestFromUnloadQueryResultGenerator implements SlicedManifestGeneratorInterface
{
    private StorageClient $gcsClient;

    public function __construct(StorageClient $gcsClient)
    {
        $this->gcsClient = $gcsClient;
    }

    /**
     * @param array<array{FILE_NAME: string, FILE_SIZE: string, ROW_COUNT: string}> $listOfUnloadedFiles
     * @throws JsonException
     */
    public function generateAndSaveManifest(RelativePathInterface $path, array $listOfUnloadedFiles = []): void
    {
        $entries = [];
        $bucket = $this->gcsClient->bucket($path->getRoot());
        foreach ($listOfUnloadedFiles as $object) {
            $entries[] = [
                'url' => sprintf('gs://%s/%s', $path->getPath(), $object['FILE_NAME']),
                'mandatory' => true,
            ];
        }
        $manifest = [
            'entries' => $entries,
        ];
        $writeStream = new WriteStream(null, [
            'chunkSize' => self::CHUNK_SIZE_256_KB,
        ]);
        $uploader = $bucket->getStreamableUploader($writeStream, [
            'name' => $path->getPathnameWithoutRoot() . 'manifest',
        ]);
        $writeStream->setUploader($uploader);
        $stream = fopen('data://text/plain,' . json_encode($manifest, JSON_THROW_ON_ERROR), 'r');
        if ($stream !== false) {
            while (($line = stream_get_line($stream, self::CHUNK_SIZE_256_KB)) !== false) {
                $writeStream->write($line);
            }
        }
        $writeStream->close();
    }
}
