<?php

declare(strict_types=1);

namespace Keboola\Provisioning\Azure;

use Symfony\Component\Process\Exception\ProcessFailedException;
use Symfony\Component\Process\Process;

final class AzCli
{
    private string $azureServicePrincipalTenant;

    private string $azureServicePrincipal;

    private string $azureServicePrincipalPassword;

    public function __construct(
        string $azureServicePrincipalTenant,
        string $azureServicePrincipal,
        string $azureServicePrincipalPassword
    ) {
        $this->azureServicePrincipalTenant = $azureServicePrincipalTenant;
        $this->azureServicePrincipal = $azureServicePrincipal;
        $this->azureServicePrincipalPassword = $azureServicePrincipalPassword;
    }

    public function runAuthorizedSingleLineOutputCmd(string $cmd): string
    {
        $out = $this->runAuthorizedCmd(
            $cmd
        );
        return explode(PHP_EOL, $out)[0];
    }

    public function runAuthorizedCmd(string $cmd): string
    {
        $volume = __DIR__ . '/../../../';
        $cmd = <<< EOT
    docker run --volume $volume:/keboola \
        -e AZURE_SERVICE_PRINCIPAL_PASSWORD=$this->azureServicePrincipalPassword \
        -e AZURE_SERVICE_PRINCIPAL=$this->azureServicePrincipal \
        -e AZURE_SERVICE_PRINCIPAL_TENANT=$this->azureServicePrincipalTenant \
        quay.io/keboola/azure-cli \
        sh -c 'az login --service-principal -u \$AZURE_SERVICE_PRINCIPAL -p \$AZURE_SERVICE_PRINCIPAL_PASSWORD --tenant \$AZURE_SERVICE_PRINCIPAL_TENANT >> /dev/null && $cmd'
EOT;

        $process = Process::fromShellCommandline($cmd);
        $process->setTimeout(3600);
        $process->setIdleTimeout(3600);
        $process->run();

        if (!$process->isSuccessful()) {
            throw new ProcessFailedException($process);
        }

        return $process->getOutput();
    }
}
