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

    /** @var mixed[]|null */
    private ?array $logTableContent;

    /**
     * @param mixed[]|null $logTableContent
     */
    public function __construct(
        string $stdErr,
        string $stdOut,
        ?int $exitCode = null,
        ?string $logContent = null,
        ?array $logTableContent = null,
    ) {
        parent::__construct(
            "Teradata TPT load ended with Error. \n\n 
        StdErr :$stdErr \n\n 
        stdOut :$stdOut \n\n 
        logContent :$logContent \n\n 
        logTableContent : "
            . ($logTableContent ? json_encode($logTableContent, JSON_THROW_ON_ERROR) : 'no data') . " \n\n",
            $exitCode ?? 0,
        );
        $this->stdErr = $stdErr;
        $this->stdOut = $stdOut;
        $this->logContent = $logContent;
        $this->exitCode = $exitCode;
        $this->logTableContent = $logTableContent;
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

    /**
     * @return array|string[]|null
     */
    public function getLogTableContent(): ?array
    {
        return $this->logTableContent;
    }
}
