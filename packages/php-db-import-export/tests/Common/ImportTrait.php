<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportCommon;

trait ImportTrait
{
    protected static function getSourceSchemaName(): string
    {
        $buildPrefix = '';
        if (getenv('BUILD_PREFIX') !== false) {
            $buildPrefix = getenv('BUILD_PREFIX');
        }

        return static::getSourceSchema()
            . '-'
            . $buildPrefix
            . '-'
            . getenv('SUITE');
    }

    protected static function getDestinationSchemaName(): string
    {
        $buildPrefix = '';
        if (getenv('BUILD_PREFIX') !== false) {
            $buildPrefix = getenv('BUILD_PREFIX');
        }

        return static::getDestinationSchema()
            . '-'
            . $buildPrefix
            . '-'
            . getenv('SUITE');
    }

    abstract protected static function getDestinationSchema(): string;

    abstract protected static function getSourceSchema(): string;
}
