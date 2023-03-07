<?php

declare(strict_types=1);

namespace Keboola\StorageBackend;

use PharIo\Version\Version;
use Symplify\MonorepoBuilder\Release\Contract\ReleaseWorker\ReleaseWorkerInterface;
use Symplify\MonorepoBuilder\Release\Process\ProcessRunner;

final class AddTagPerPackagesWorker implements ReleaseWorkerInterface
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
            // e.g. php-datatypes/7.0.0, php-table-backend-utils/7.0.0 ...
            $tagName = $directory . '/' . $version->getOriginalString();
            $this->processRunner->run('git tag ' . $tagName);
        }
    }

    public function getDescription(Version $version): string
    {
        return sprintf('Add local tag "%s" for all libraries with a prefix for each lib. e.g. `php-datatypes/7.0.0`, `php-table-backend-utils/7.0.0` ...', $version->getOriginalString());
    }
}
