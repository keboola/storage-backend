<?php

declare(strict_types=1);

/**
 * Loads test fixtures into ABS
 */

date_default_timezone_set('Europe/Prague');
ini_set('display_errors', '1');
error_reporting(E_ALL);

$basedir = dirname(__DIR__);

require_once $basedir . '/vendor/autoload.php';

$accountName = (string) getenv('ABS_ACCOUNT_NAME');
$container = (string) getenv('ABS_CONTAINER_NAME');

$connString = sprintf(
    'DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net',
    $accountName,
    getenv('ABS_ACCOUNT_KEY')
);

echo "Creating blob service \n";
$blobClient = \MicrosoftAzure\Storage\Blob\BlobRestProxy::createBlobService($connString);

try {
    echo "Deleting a previous container \n";
    $blobClient->deleteContainer($container);
    sleep(1);
} catch (\MicrosoftAzure\Storage\Common\Exceptions\ServiceException $e) {
    if (preg_match('~The specified container does not exist~', $e->getMessage())) {
        echo "Container does not exists. Deleting skipped\n";
    } else {
        throw $e;
    }
}

$created = false;
while ($created === false) {
    try {
        echo "Creating a container \n";
        $blobClient->createContainer($container);
        echo "Container created \n";
        $created = true;
    } catch (\MicrosoftAzure\Storage\Common\Exceptions\ServiceException $e) {
        if (preg_match('~The specified container is being deleted.~', $e->getMessage())) {
            echo "Waiting, because old container is being deleted ... \n";
            sleep(2);
        } else {
            throw  $e;
        }
    }
}

echo "Creating blobs ...\n";


$blobClient->createBlockBlob(
    $container,
    'file.csv',
    file_get_contents(__DIR__ . '/data/file.csv')
);

$finder = new \Symfony\Component\Finder\Finder();
$files = $finder->in(__DIR__ . '/data/sliced')->files()->name('sliced.csv_*');

$manifest = ['entries' => []];

foreach ($files as $file) {
    $path = 'sliced/' . $file->getFilename();
    $blobClient->createBlockBlob($container, $path, file_get_contents($file->getPathname()));
    $manifest['entries'][] = [
        'url' => sprintf(
            'azure://%s.%s/%s/%s',
            $accountName,
            \MicrosoftAzure\Storage\Common\Internal\Resources::BLOB_BASE_DNS_NAME,
            $container,
            $path
        ),
    ];
}
$manifestFilePath = __DIR__ . '/data/sliced/sliced.csvmanifest';
$manifestFile = file_put_contents($manifestFilePath, \GuzzleHttp\json_encode($manifest));

$blobClient->createBlockBlob($container, 'sliced/sliced.csvmanifest', file_get_contents($manifestFilePath));

echo "ABS load complete \n";
