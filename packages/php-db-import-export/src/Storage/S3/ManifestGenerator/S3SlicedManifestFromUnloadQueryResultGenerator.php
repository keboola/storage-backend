<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\S3\ManifestGenerator;

use Aws\S3\S3Client;
use JsonException;
use Keboola\Db\ImportExport\Storage\SlicedManifestGeneratorInterface;
use Keboola\FileStorage\Path\RelativePathInterface;

/**
 * Generates manifest file from list of files originated in snowflake unload statement
 */
class S3SlicedManifestFromUnloadQueryResultGenerator implements SlicedManifestGeneratorInterface
{
    private S3Client $s3Client;

    public function __construct(S3Client $s3Client)
    {
        $this->s3Client = $s3Client;
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
                'url' => sprintf('s3://%s/%s', $path->getPath(), $object['FILE_NAME']),
                'mandatory' => true,
            ];
        }
        $manifest = [
            'entries' => $entries,
        ];

        $this->s3Client->putObject([
            'Bucket' => $path->getRoot(),
            'Key' => $path->getPathnameWithoutRoot() . 'manifest',
            'Body' => json_encode($manifest, JSON_THROW_ON_ERROR),
            'ServerSideEncryption' => 'AES256',
        ]);
    }
}
