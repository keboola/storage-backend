<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage;

use Keboola\FileStorage\Path\RelativePathInterface;

interface SlicedManifestGeneratorInterface
{
    public const CHUNK_SIZE_256_KB = 1024 * 256; // 256KB
    public function generateAndSaveManifest(RelativePathInterface $path): void;
}
