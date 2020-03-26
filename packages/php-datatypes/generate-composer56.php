<?php

$composerFile = __DIR__ . '/composer.json';

$data = json_decode(file_get_contents($composerFile), true);
$data['require-dev'] = [
    'php-parallel-lint/php-parallel-lint' => '^1.1'
];
$data['scripts'] = array_filter($data['scripts'], function ($item) {
    return in_array($item, ['phplint', 'build', 'ci']);
}, ARRAY_FILTER_USE_KEY);
$data['scripts']['build'] = ['@phplint'];

file_put_contents($composerFile, json_encode($data, JSON_UNESCAPED_SLASHES));