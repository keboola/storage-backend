<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata;

use Keboola\Db\ImportExport\ImportOptions;

class TeradataImportOptions extends ImportOptions
{
    private string $teradataHost;

    private string $teradataUser;

    private string $teradataPassword;

    private int $teradataPort;

    /**
     * @param string[] $convertEmptyValuesToNull
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
        string $usingTypes = self::USING_TYPES_STRING,
    ) {
        parent::__construct(
            convertEmptyValuesToNull: $convertEmptyValuesToNull,
            isIncremental: $isIncremental,
            useTimestamp: $useTimestamp,
            numberOfIgnoredLines: $numberOfIgnoredLines,
            usingTypes: $usingTypes,
            importAsNull: [], // Teradata does not support importAsNull now
        );
        $this->teradataHost = $teradataHost;
        $this->teradataUser = $teradataUser;
        $this->teradataPassword = $teradataPassword;
        $this->teradataPort = $teradataPort;
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
}
