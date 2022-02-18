<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\ToStage\Exception;

use Keboola\Db\ImportExport\Exception\Exception;

class FailedTPTLoadException extends Exception
{
    private string $stdErr;

    private string $stdOut;

    private ?string $logContent;

    private ?int $exitCode;

    private ?array $logTableContent;

    private ?array $errTableContent;

    private ?array $errTable2Content;

    public function __construct(
        string $stdErr,
        string $stdOut,
        ?int $exitCode,
        ?string $logContent,
        ?array $logTableContent,
        ?array $errTableContent,
        ?array $errTable2Content
    ) {
        parent::__construct('Teradata TPT load ended with Error.', $exitCode ?? 0);
        $this->stdErr = $stdErr;
        $this->stdOut = $stdOut;
        $this->logContent = $logContent;
        $this->exitCode = $exitCode;
        $this->logTableContent = $logTableContent;
        $this->errTableContent = $errTableContent;
        $this->errTable2Content = $errTable2Content;
    }

    public function getStdErr(): string
    {
        return $this->stdErr;
    }

    public function getStdOut(): string
    {
        return $this->stdOut;
    }

    public function getLogContent(): ?string
    {
        return $this->logContent;
    }

    public function getExitCode(): ?int
    {
        return $this->exitCode;
    }

    public function getLogTableContent(): ?array
    {
        return $this->logTableContent;
    }

    public function getErrTableContent(): ?array
    {
        return $this->errTableContent;
    }

    public function getErrTable2Content(): ?array
    {
        return $this->errTable2Content;
    }
}
