<?php

declare(strict_types=1);

namespace Keboola\StorageBackend;

use PharIo\Version\Version;
use Symplify\MonorepoBuilder\Release\Contract\ReleaseWorker\ReleaseWorkerInterface;
use Symplify\MonorepoBuilder\Release\Process\ProcessRunner;

final class AddTagPerPackagesWhenRepoIsReleasedWorker implements ReleaseWorkerInterface
{
    /**
     * @var \Symplify\MonorepoBuilder\Release\Process\ProcessRunner
     */
    private $processRunner;
    public function __construct(ProcessRunner $processRunner)
    {
        $this->processRunner = $processRunner;
    }
    public function work(Version $version) : void
    {
//      I have to go through the packages folder, from composer.json it's not possible, they often have different names than the repository itself, what I need for split script
        $directories = glob('/code/packages/*' , GLOB_ONLYDIR);
        $directories = array_map(function ($item) {
            return str_replace('/code/packages/', '', $item);
        }, $directories);

        foreach ($directories as $directory) {
            $this->processRunner->run('git tag ' . $directory . '/' . $version->getOriginalString());
        }
    }

    public function getDescription(Version $version): string
    {
        return 'From the `/packages` folder, it takes all the packages and creates a tag for them on release.';
    }
}
