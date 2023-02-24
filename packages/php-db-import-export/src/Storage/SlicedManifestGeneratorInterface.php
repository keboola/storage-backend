<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage;

use Keboola\FileStorage\Path\RelativePathInterface;

interface SlicedManifestGeneratorInterface
{
    public function generateAndSaveManifest(RelativePathInterface $path): void;
}
