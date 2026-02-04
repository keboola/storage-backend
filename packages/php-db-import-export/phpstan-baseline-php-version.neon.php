<?php

declare(strict_types=1);

$includes = [];
$includes[] = __DIR__ . '/phpstan-baseline.neon';

$config = [];
$config['includes'] = $includes;
$config['parameters']['phpVersion'] = PHP_VERSION_ID;

return $config;
