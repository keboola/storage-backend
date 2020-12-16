<?php

declare(strict_types=1);

namespace Keboola\Provisioning;

use Keboola\Provisioning\Azure\AzCli;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Process\Exception\ProcessFailedException;
use Symfony\Component\Process\Process;

abstract class BaseCmd extends Command
{
    private const OPTION_AZURE_SERVICE_PRINCIPAL = 'principalName';
    private const OPTION_AZURE_SERVICE_PRINCIPAL_PASSWORD = 'principalPassword';
    private const OPTION_AZURE_SERVICE_PRINCIPAL_TENANT = 'principalTenant';

    protected function configure(): void
    {
        $this
            ->addOption(
                self::OPTION_AZURE_SERVICE_PRINCIPAL_TENANT,
                null,
                InputOption::VALUE_REQUIRED
            );

        $this
            ->addOption(
                self::OPTION_AZURE_SERVICE_PRINCIPAL,
                null,
                InputOption::VALUE_REQUIRED
            );

        $this
            ->addOption(
                self::OPTION_AZURE_SERVICE_PRINCIPAL_PASSWORD,
                null,
                InputOption::VALUE_REQUIRED
            );
    }

    protected function getCli(InputInterface $input): AzCli
    {
        [
            $principalTenant,
            $principalName,
            $principalPassword,
        ] = $this->getOptions($input);
        return new AzCli(
            $principalTenant,
            $principalName,
            $principalPassword,
        );
    }

    protected function getOptions(InputInterface $input): array
    {
        $principalTenant = $input->getOption(self::OPTION_AZURE_SERVICE_PRINCIPAL_TENANT);
        $this->assertOption(self::OPTION_AZURE_SERVICE_PRINCIPAL_TENANT, $principalTenant);
        $principalName = $input->getOption(self::OPTION_AZURE_SERVICE_PRINCIPAL);
        $this->assertOption(self::OPTION_AZURE_SERVICE_PRINCIPAL, $principalName);
        $principalPassword = $input->getOption(self::OPTION_AZURE_SERVICE_PRINCIPAL_PASSWORD);
        $this->assertOption(self::OPTION_AZURE_SERVICE_PRINCIPAL_PASSWORD, $principalPassword);

        return [
            $principalTenant,
            $principalName,
            $principalPassword,
        ];
    }

    protected function assertOption(string $optionName, $optionValue): void
    {
        if (is_string($optionValue)) {
            return;
        }
        throw new \LogicException(sprintf('Option "%s" must be string.', $optionName));
    }

    protected function runCmdSingleLineOutput(string $cmd): string
    {
        $out = $this->runCmd($cmd);
        return explode(PHP_EOL, $out)[0];
    }

    protected function runCmd(string $cmd): string
    {
        $process = Process::fromShellCommandline($cmd);
        $process->run();

        if (!$process->isSuccessful()) {
            throw new ProcessFailedException($process);
        }

        return $process->getOutput();
    }
}
