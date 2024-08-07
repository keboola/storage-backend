<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\GCS\ManifestGenerator;

use Google\Cloud\Storage\WriteStream;
use Keboola\Db\ImportExport\Storage\SlicedManifestGeneratorInterface;

class WriteStreamFactory
{
    public function createWriteStream(): WriteStream
    {
        return new WriteStream(null, [
            'chunkSize' => SlicedManifestGeneratorInterface::CHUNK_SIZE_256_KB,
        ]);
    }
}
