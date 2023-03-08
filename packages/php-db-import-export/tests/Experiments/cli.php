#!/usr/bin/env php
<?php
declare(strict_types=1);

require __DIR__ . '/../../vendor/autoload.php';

use Symfony\Component\Console\Application;

set_error_handler(function($errno, $errstr, $errfile, $errline ) {
    throw new ErrorException($errstr, $errno, 0, $errfile, $errline);
});

$application = new Application();

$application->add(new \Tests\Keboola\Db\ImportExportExperiments\Snowflake\ImportTypeNull());
$application->run();
