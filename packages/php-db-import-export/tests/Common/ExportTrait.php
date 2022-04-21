<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportCommon;

trait ExportTrait
{
    protected function getExportDir(): string
    {
        $buildPrefix = '';
        if (getenv('BUILD_PREFIX') !== false) {
            $buildPrefix = getenv('BUILD_PREFIX');
        }

        return $this->getExportBlobDir()
            . '-'
            . $buildPrefix
            . '-'
            . getenv('SUITE');
    }

    abstract protected function getExportBlobDir(): string;
}
