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
        __DIR__ . '/packages/php-storage-driver-common/contract',
        __DIR__ . '/packages/php-storage-driver-common/Shared',
        __DIR__ . '/packages/php-storage-driver-common/tests',
        __DIR__ . '/packages/php-storage-driver-snowflake/src',
        __DIR__ . '/packages/php-storage-driver-snowflake/tests',
    ])
    ->withSkip([
        __DIR__ . '/packages/php-storage-driver-common/generated',
        __DIR__ . '/packages/php-storage-driver-common/tests/generated',
    ])
    ->withSets([
        PHPUnitSetList::ANNOTATIONS_TO_ATTRIBUTES,
        PHPUnitSetList::PHPUNIT_100,
    ]);
