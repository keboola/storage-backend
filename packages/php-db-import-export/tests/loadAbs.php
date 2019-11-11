<?php

declare(strict_types=1);

/**
 * Loads test fixtures into ABS
 */

use Tests\Keboola\Db\ImportExport\AbsLoader;

date_default_timezone_set('Europe/Prague');
ini_set('display_errors', '1');
error_reporting(E_ALL);

$basedir = dirname(__DIR__);

require_once $basedir . '/vendor/autoload.php';
require_once 'AbsLoader.php';

$loader = new AbsLoader(
    (string) getenv('ABS_ACCOUNT_NAME'),
    (string) getenv('ABS_CONTAINER_NAME')
);
$loader->deleteContainer();
$loader->createContainer();
$loader->load();
