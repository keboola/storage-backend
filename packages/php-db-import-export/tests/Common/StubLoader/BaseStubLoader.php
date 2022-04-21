<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportCommon\StubLoader;

use Symfony\Component\Finder\Finder;

abstract class BaseStubLoader
{
    public const BASE_DIR = __DIR__ . '/../../data/';

    public function generateLargeSliced(): void
    {
        $finder = new Finder();
        $finder->files()->in(self::BASE_DIR . 'sliced/2cols-large/');
        if ($finder->count() > 1000) {
            // files are generated
            return;
        }
        for ($i = 0; $i <= 1500; $i++) {
            $sliceName = sprintf('sliced.csv_%d', $i);
            file_put_contents(
                self::BASE_DIR . 'sliced/2cols-large/' . $sliceName,
                "\"a\",\"b\"\n"
            );
        }
    }

    abstract public function load(): void;
}
