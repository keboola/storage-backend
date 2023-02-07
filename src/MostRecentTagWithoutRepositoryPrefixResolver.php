<?php

declare(strict_types=1);

namespace Keboola\StorageBackend;


use Symplify\MonorepoBuilder\Contract\Git\TagResolverInterface;
use Symplify\MonorepoBuilder\Release\Process\ProcessRunner;

class MostRecentTagWithoutRepositoryPrefixResolver implements TagResolverInterface
{
    /**
     * @var string[]
     */
    private const COMMAND = ['git', 'tag', '-l', '--sort=committerdate'];
    /**
     * @var \Symplify\MonorepoBuilder\Release\Process\ProcessRunner
     */
    private $processRunner;
    public function __construct(ProcessRunner $processRunner)
    {
        $this->processRunner = $processRunner;
    }
    /**
     * Returns null, when there are no local tags yet
     */
    public function resolve(string $gitDirectory) : ?string
    {
        $tagList = $this->parseTags($this->processRunner->run(self::COMMAND, $gitDirectory));
        /** @var string $theMostRecentTag */
        $theMostRecentTag = (string) \array_pop($tagList);
        if ($theMostRecentTag === '') {
            return null;
        }
        return $theMostRecentTag;
    }
    /**
     * @return string[]
     */
    private function parseTags(string $commandResult) : array
    {
        $tags = \trim($commandResult);
        // Remove all "\r" chars in case the CLI env like the Windows OS.
        // Otherwise (ConEmu, git bash, mingw cli, e.g.), leave as is.
        $normalizedTags = \str_replace("\r", '', $tags);
        $tagsArray = \explode("\n", $normalizedTags);

        // Git history contains tags that belonged to repositories before they were in monorepo.
        // These tags are prefixed with the repository name (php-datatypes/*, etc.) and we don't want to include them.
        return array_filter($tagsArray, function ($tag) {
            return preg_match('/^[0-9]+\.[0-9]+\.[0-9]+$/', $tag);
        });
    }
}
