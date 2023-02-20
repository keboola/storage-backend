<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage;

use Keboola\FileStorage\Path\RelativePathInterface;

interface DestinationFileInterface extends DestinationInterface
{
    /**
     * @return mixed
     */
    public function getClient();

    public function getRelativePath(): RelativePathInterface;
}
