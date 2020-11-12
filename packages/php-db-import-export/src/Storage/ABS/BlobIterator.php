<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\Blob;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;

final class BlobIterator implements \Iterator
{
    private const DEFAULT_PAGE_SIZE = 1000;

    /** @var BlobRestProxy */
    private $client;

    /** @var ListBlobsOptions */
    private $options;

    /** @var string */
    private $container;

    /** @var string|null */
    private $offsetMarker;

    /** @var int */
    private $position = 0;

    /** @var int */
    private $currentListIndexPointer = 0;

    /** @var Blob[] */
    private $blobs = [];

    /** @var bool */
    private $isFirstPageLoaded = false;

    public function __construct(
        BlobRestProxy $blobRestProxy,
        string $container,
        ?ListBlobsOptions $options = null
    ) {
        $this->client = $blobRestProxy;
        if ($options === null) {
            $options = new ListBlobsOptions();
        }
        /** @var int|null $maxResults */
        $maxResults = $options->getMaxResults();
        if ($maxResults === null) {
            $options->setMaxResults(self::DEFAULT_PAGE_SIZE);
        }
        $this->options = $options;
        $this->container = $container;
    }

    public function current(): Blob
    {
        return $this->blobs[$this->currentListIndexPointer];
    }

    public function key(): int
    {
        return $this->position;
    }

    public function next(): void
    {
        ++$this->position;
        ++$this->currentListIndexPointer;
    }

    public function rewind(): void
    {
        $this->isFirstPageLoaded = false;
        $this->currentListIndexPointer = 0;
        $this->position = 0;
        $this->offsetMarker = null;
        $this->blobs = [];
    }

    public function valid(): bool
    {
        if ($this->isFirstPageLoaded === false) {
            $this->loadNextPage();
        }

        if (array_key_exists($this->currentListIndexPointer, $this->blobs)) {
            return true;
        }

        if ($this->offsetMarker === null) {
            return false;
        }

        $this->loadNextPage();
        if (array_key_exists($this->currentListIndexPointer, $this->blobs)) {
            return true;
        }

        return false;
    }

    private function loadNextPage(): void
    {
        $this->options->setMarker($this->offsetMarker);
        $result = $this->client->listBlobs($this->container, $this->options);
        $this->isFirstPageLoaded = true;
        $this->currentListIndexPointer = 0;
        $this->offsetMarker = $result->getNextMarker();
        $this->blobs = $result->getBlobs();
    }
}
