<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit;

use PHPUnit\Framework\TestCase;

abstract class BaseTestCase extends TestCase
{
    protected const DATA_DIR = __DIR__ . '/../data/';
}
