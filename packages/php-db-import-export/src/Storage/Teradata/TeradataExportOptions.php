<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Teradata;

use Keboola\Db\ImportExport\ExportOptions;

class TeradataExportOptions extends ExportOptions
{
    public const DEFAULT_BUFFER_SIZE = '8M';
    public const DEFAULT_MAX_OBJECT_SIZE = '8000M'; // default of TD BUFFER size * 1000
    public const DEFAULT_SINGLE_PART_FILE = false;
    public const DEFAULT_SPLIT_ROWS = true;

    private string $teradataHost;

    private string $teradataUser;

    private string $teradataPassword;

    private int $teradataPort;

    // TD settings for sliced files
    private string $bufferSize;

    private string $maxObjectSize;

    private bool $dontSplitRows;

    private bool $singlePartFile;

    public function __construct(
        string $teradataHost,
        string $teradataUser,
        string $teradataPassword,
        int $teradataPort,
        bool $isCompressed,
        string $bufferSize = self::DEFAULT_BUFFER_SIZE,
        string $maxObjectSize = self::DEFAULT_MAX_OBJECT_SIZE,
        bool $dontSplitRows = self::DEFAULT_SPLIT_ROWS,
        bool $singlePartFile = self::DEFAULT_SINGLE_PART_FILE
    ) {
        parent::__construct(
            $isCompressed
        );
        $this->teradataHost = $teradataHost;
        $this->teradataUser = $teradataUser;
        $this->teradataPassword = $teradataPassword;
        $this->teradataPort = $teradataPort;

        $this->bufferSize = $bufferSize;
        $this->maxObjectSize = $maxObjectSize;
        $this->dontSplitRows = $dontSplitRows;
        $this->singlePartFile = $singlePartFile;
    }

    public function getBufferSize(): string
    {
        return $this->bufferSize;
    }

    public function getMaxObjectSize(): string
    {
        return $this->maxObjectSize;
    }

    public function getTeradataHost(): string
    {
        return $this->teradataHost;
    }

    public function getTeradataUser(): string
    {
        return $this->teradataUser;
    }

    public function getTeradataPassword(): string
    {
        return $this->teradataPassword;
    }

    public function getTeradataPort(): int
    {
        return $this->teradataPort;
    }

    /**
     * @return array<int, string>
     */
    public function getTeradataCredentials(): array
    {
        return [
            $this->teradataHost,
            $this->teradataUser,
            $this->teradataPassword,
        ];
    }

    /**
     * generates part of TPT which defines settings for sliced files
     */
    public function generateS3SizeOptions(): string
    {
        return sprintf(
            'S3DontSplitRows=%s S3SinglePartFile=%s S3MaxObjectSize=%s S3BufferSize=%s',
            $this->dontSplitRows ? 'True' : 'False',
            $this->singlePartFile ? 'True' : 'False',
            $this->maxObjectSize,
            $this->bufferSize
        );
    }
}
