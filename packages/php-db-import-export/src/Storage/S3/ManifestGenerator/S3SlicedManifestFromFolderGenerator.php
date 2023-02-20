<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\S3\ManifestGenerator;

use Aws\S3\S3Client;
use JsonException;
use Keboola\Db\ImportExport\Storage\SlicedManifestGeneratorInterface;
use Keboola\FileStorage\Path\RelativePathInterface;

/**
 * Generates manifest file by iterating files in a s3 prefix
 */
class S3SlicedManifestFromFolderGenerator implements SlicedManifestGeneratorInterface
{
    private S3Client $s3Client;

    public function __construct(S3Client $s3Client)
    {
        $this->s3Client = $s3Client;
    }

    /**
     * @throws JsonException
     */
    public function generateAndSaveManifest(RelativePathInterface $path): void
    {
        $iterator = $this->s3Client->getIterator('ListObjects', [
            'Bucket' => $path->getRoot(),
            'Prefix' => $path->getPathnameWithoutRoot(),
        ]);
        $entries = [];
        /** @var array{Key:string} $object */
        foreach ($iterator as $object) {
            $entries[] = [
                'url' => sprintf('s3://%s/%s', $path->getRoot(), $object['Key']),
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
