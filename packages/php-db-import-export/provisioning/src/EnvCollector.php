<?php

declare(strict_types=1);

namespace Keboola\Provisioning;

use Symfony\Component\Filesystem\Filesystem;

final class EnvCollector
{
    private string $envFile;

    public function __construct()
    {
        $this->envFile = __DIR__ . '/../env_export';
    }

    public function addEnv(string $name, string $value): void
    {
        $fs = new Filesystem();
        if (!$fs->exists($this->envFile)) {
            $fs->touch($this->envFile);
        }
        $fs->appendToFile($this->envFile, sprintf('export %s=%s' . PHP_EOL, $name, $value));
    }
}
