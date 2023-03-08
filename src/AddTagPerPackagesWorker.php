<?php

declare(strict_types=1);

namespace Keboola\StorageBackend;

use MonorepoBuilderPrefix202301\Symplify\PackageBuilder\Parameter\ParameterProvider;
use MonorepoBuilderPrefix202301\Symplify\SmartFileSystem\SmartFileInfo;
use PharIo\Version\Version;
use Symplify\MonorepoBuilder\FileSystem\ComposerJsonProvider;
use Symplify\MonorepoBuilder\Release\Contract\ReleaseWorker\ReleaseWorkerInterface;
use Symplify\MonorepoBuilder\Release\Process\ProcessRunner;
use Symplify\MonorepoBuilder\ValueObject\Option;

final class AddTagPerPackagesWorker implements ReleaseWorkerInterface
{
    private ProcessRunner $processRunner;

    private ComposerJsonProvider $composerJsonProvider;

    /**
     * @var string[]
     */
    private array $packageDirectoriesExcludes;

    public function __construct(
        ProcessRunner $processRunner,
        ComposerJsonProvider $composerJsonProvider,
        ParameterProvider $parameterProvider
    ) {
        $this->processRunner = $processRunner;
        $this->composerJsonProvider = $composerJsonProvider;
        $this->packageDirectoriesExcludes = $parameterProvider->provideArrayParameter(Option::PACKAGE_DIRECTORIES_EXCLUDES);
    }
    public function work(Version $version) : void
    {
        $packagesFileInfos = $this->getPackagesFileInfos();

        foreach ($packagesFileInfos as $packagesFileInfo) {
            // e.g. php-datatypes/7.0.0, php-table-backend-utils/7.0.0 ...
            $tagName = $this->generatePackageTagName($packagesFileInfo, $version);
            $this->processRunner->run('git tag ' . $tagName);
        }
    }

    public function getDescription(Version $version): string
    {
        return sprintf('Add local tag "%s" for all libraries with a prefix for each lib. e.g. `php-datatypes/7.0.0`, `php-table-backend-utils/7.0.0` ...', $version->getOriginalString());
    }

    /**
     * @return SmartFileInfo[]
     */
    private function getPackagesFileInfos(): array
    {
        // return all packages except those in excluded directories
        return array_filter($this->composerJsonProvider->getPackagesComposerFileInfos(), function ($packageFileInfo) {
            foreach ($this->packageDirectoriesExcludes as $packageDirectoryExclude) {
                if (strpos($packageFileInfo->getPath(), $packageDirectoryExclude) !== false) {
                    return false;
                }
            }
            return true;
        });
    }

    private function generatePackageTagName(SmartFileInfo $smartFileInfo, Version $version): string
    {
        return basename($smartFileInfo->getRelativeDirectoryPath()) . '/' . $version->getOriginalString();
    }
}
