<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Teradata;

use Keboola\Db\ImportExport\ExportOptions;

class TeradataExportOptions extends ExportOptions
{
    private string $teradataHost;

    private string $teradataUser;

    private string $teradataPassword;

    private int $teradataPort;

    public function __construct(
        string $teradataHost,
        string $teradataUser,
        string $teradataPassword,
        int $teradataPort,
        bool $isCompressed
    ) {
        parent::__construct(
            $isCompressed
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

    public function getTeradataCredentials(): array
    {
        return [
            $this->teradataHost,
            $this->teradataUser,
            $this->teradataPassword,
        ];
    }
}
