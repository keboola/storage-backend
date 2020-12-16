#!/usr/bin/env php
<?php

require __DIR__.'/vendor/autoload.php';

use Keboola\Provisioning\DeleteSynapse;
use Keboola\Provisioning\DeploySynapse;
use Symfony\Component\Console\Application;

$application = new Application();

$application->add(new DeploySynapse());
$application->add(new DeleteSynapse());

$application->run();
