<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportCommon;

trait ImportTrait
{
    protected function getSourceSchemaName(): string
    {
        $buildPrefix = '';
        if (getenv('BUILD_PREFIX') !== false) {
            $buildPrefix = getenv('BUILD_PREFIX');
        }

        return $this->getSourceSchema()
            . '-'
            . $buildPrefix
            . '-'
            . getenv('SUITE');
    }

    protected function getDestinationSchemaName(): string
    {
        $buildPrefix = '';
        if (getenv('BUILD_PREFIX') !== false) {
            $buildPrefix = getenv('BUILD_PREFIX');
        }

        return $this->getDestinationSchema()
            . '-'
            . $buildPrefix
            . '-'
            . getenv('SUITE');
    }

    abstract protected function getDestinationSchema(): string;

    abstract protected function getSourceSchema(): string;
}
