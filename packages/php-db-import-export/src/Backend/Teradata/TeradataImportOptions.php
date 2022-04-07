<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata;

use Keboola\Db\ImportExport\ImportOptions;

class TeradataImportOptions extends ImportOptions
{
    public const CSV_ADAPTER_TPT = 'TPT';
    public const CSV_ADAPTER_SPT = 'SPT';
    private const DEFAULT_CSV_ADAPTER = self::CSV_ADAPTER_TPT;

    private string $teradataHost;

    private string $teradataUser;

    private string $teradataPassword;

    private int $teradataPort;

    /**
     * @var TeradataImportOptions::CSV_ADAPTER_*
     */
    private string $csvImportAdapter;

    /**
     * @param string[] $convertEmptyValuesToNull
     * @param TeradataImportOptions::CSV_ADAPTER_* $csvImportAdapter
     */
    public function __construct(
        string $teradataHost,
        string $teradataUser,
        string $teradataPassword,
        int $teradataPort,
        array $convertEmptyValuesToNull = [],
        bool $isIncremental = false,
        bool $useTimestamp = false,
        int $numberOfIgnoredLines = 0,
        string $csvImportAdapter = self::DEFAULT_CSV_ADAPTER
    ) {
        parent::__construct(
            $convertEmptyValuesToNull,
            $isIncremental,
            $useTimestamp,
            $numberOfIgnoredLines
        );
        $this->teradataHost = $teradataHost;
        $this->teradataUser = $teradataUser;
        $this->teradataPassword = $teradataPassword;
        $this->teradataPort = $teradataPort;
        $this->csvImportAdapter = $csvImportAdapter;
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
     * @return TeradataImportOptions::CSV_ADAPTER_*
     */
    public function getCsvImportAdapter(): string
    {
        return $this->csvImportAdapter;
    }
}
