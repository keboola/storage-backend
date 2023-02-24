<?php

declare(strict_types=1);

use Keboola\StorageDriver\TestsStubLoader\S3Loader;

date_default_timezone_set('Europe/Prague');
ini_set('display_errors', '1');
error_reporting(E_ALL);

$basedir = dirname(__DIR__);

require_once $basedir . '/../vendor/autoload.php';

switch ($argv[1]) {
    case 's3':
        require_once 'S3Loader.php';

        $loader = new S3Loader(
            (string) getenv('AWS_REGION'),
            (string) getenv('AWS_S3_BUCKET')
        );
        $loader->clearBucket();
        $loader->load();
        break;
    default:
        throw new Exception('Only abs|s3 options are supported.');
}
