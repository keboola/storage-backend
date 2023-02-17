<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\S3;

use Keboola\Db\ImportExport\Storage\DestinationFileInterface;
use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\FileStorage\Path\RelativePath;
use Keboola\FileStorage\Path\RelativePathInterface;
use Keboola\FileStorage\S3\S3Provider;

class DestinationFile extends BaseFile implements DestinationFileInterface
{
    private const N_OF_FILES_COMPRESSED = 10;
    private const N_OF_FILES_UNCOMPRESSED = 32; // <- same limit as for import

    /**
     * Method exists to pre-generate file names for Exasol export
     *
     * @return string[]
     */
    public function getSlicedFilesNames(bool $isCompressed): array
    {
        $path = $this->getRelativePath();

        $filename = $path->getPathnameWithoutRoot();
        $suffix = $isCompressed ? '.gz' : '';
        // Exasol wont slice by default
        // for compressed files all files are created this is annoying
        // for not compressed only necessary amount of files is created
        $countOfFiles = $isCompressed ? self::N_OF_FILES_COMPRESSED : self::N_OF_FILES_UNCOMPRESSED;

        $files = [];
        for ($index = 1; $index < $countOfFiles + 1; $index++) {
            $files[] = sprintf('%s_%03d.csv%s', $filename, $index, $suffix);
        }

        return $files;
    }

    public function getRelativePath(): RelativePathInterface
    {
        return RelativePath::createFromRootAndPath(new S3Provider(), $this->bucket, $this->filePath);
    }
}
