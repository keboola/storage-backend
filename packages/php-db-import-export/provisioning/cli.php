#!/usr/bin/env php
<?php

declare(strict_types=1);

require __DIR__.'/vendor/autoload.php';

use Keboola\Provisioning\Delete;
use Keboola\Provisioning\Deploy;
use Symfony\Component\Console\Application;

$application = new Application();

$application->add(new Deploy());
$application->add(new Delete());

$application->run();
