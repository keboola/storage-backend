<?php

declare(strict_types=1);

/**
 * Set up synapse
 */
require __DIR__ . '/../vendor/autoload.php';

date_default_timezone_set('Europe/Prague');
ini_set('display_errors', '1');
error_reporting(E_ALL);

$db = \Doctrine\DBAL\DriverManager::getConnection([
    'user' => getenv('SYNAPSE_UID'),
    'password' => getenv('SYNAPSE_PWD'),
    'host' => getenv('SYNAPSE_SERVER'),
    'dbname' => getenv('SYNAPSE_DATABASE'),
    'port' => 1433,
    'driver' => 'pdo_sqlsrv',
    'driverOptions' => [
        'ConnectRetryCount' => 5,
        'ConnectRetryInterval' => 10,
    ],
]);

try {
    $db->exec('DROP MASTER KEY');
} catch (Throwable $e) {
    // ignore if fail (should always fail)
}

$db->exec('CREATE MASTER KEY');
