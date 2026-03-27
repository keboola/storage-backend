<?php

declare(strict_types=1);

use Rector\Config\RectorConfig;
use Rector\PHPUnit\Set\PHPUnitSetList;

return RectorConfig::configure()
    ->withPaths([
        __DIR__ . '/packages/php-datatypes/src',
        __DIR__ . '/packages/php-datatypes/tests',
        __DIR__ . '/packages/php-table-backend-utils/src',
        __DIR__ . '/packages/php-table-backend-utils/tests',
        __DIR__ . '/packages/php-db-import-export/src',
        __DIR__ . '/packages/php-db-import-export/tests',
    ])
    ->withSets([
        PHPUnitSetList::ANNOTATIONS_TO_ATTRIBUTES,
        PHPUnitSetList::PHPUNIT_100,
    ]);
