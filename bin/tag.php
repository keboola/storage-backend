#!/usr/bin/env php
<?php
require __DIR__ . '/../vendor/autoload.php';

use Symfony\Component\Console\Question\ChoiceQuestion;
use Symfony\Component\Console\Question\ConfirmationQuestion;
use Symfony\Component\Console\Question\Question;
use Symfony\Component\Process\Exception\ProcessFailedException;
use Symfony\Component\Process\Process;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\SingleCommandApplication;

function runCmd($commandLine, $cwd): string
{
    $p = Process::fromShellCommandline($commandLine, $cwd);
    $p->run();
    if (!$p->isSuccessful()) {
        throw new ProcessFailedException($p);
    }
    $output = $p->getOutput();
    // remove trailing new line
    return rtrim($output);
}

(new SingleCommandApplication())
    ->setName('Tag me')
    ->setCode(code: function (InputInterface $input, OutputInterface $output): int {
        $cwd = __DIR__ . '/..'; // root of repo
        $branch = runCmd('git rev-parse --abbrev-ref HEAD', $cwd);
        if ($branch === 'main') {
            throw new RuntimeException('Cannot tag main branch');
        }
        $output->writeln(sprintf('Running tag on branch "%s"', $branch),);
        $finder = new Finder();
        $packages = $finder->directories()->in($cwd . '/packages')->depth(0);

        $helper = $this->getHelper('question');
        $question = new ChoiceQuestion(
            'Select package to tag',
            array_map(static fn(SplFileInfo $i) => basename($i->getPathname()), iterator_to_array($packages)),
        );
        $question->setErrorMessage('Package %s is invalid.');

        $package = $helper->ask($input, $output, $question);
        $output->writeln(sprintf('You have just selected: "%s"', basename($package)));

        $tagPrefix = sprintf("%s/%s-", basename($package), $branch);

        $tags = runCmd(sprintf('git tag --sort=refname -l \'%s*\'', $tagPrefix), $cwd);
        $tags = explode(PHP_EOL, $tags);

        $lastNumericTagId = 0;
        if ($tags !== []) {
            foreach ($tags as $tag) {
                $tagId = str_replace($tagPrefix, '', $tag);
                if (is_numeric($tagId)) {
                    $lastNumericTagId = (int) $tagId;
                }
            }
        }
        $newTagId = $lastNumericTagId + 1;
        $question = new Question(
            sprintf(
                'Add tag suffix "%s{suffix}" manually or ignore this by pressing <enter> which will set "%s"',
                $tagPrefix,
                $tagPrefix . $newTagId
            ),
            $newTagId
        );
        $suffix = $helper->ask($input, $output, $question);
        $newTag = $tagPrefix . $suffix;

        $output->writeln(sprintf('New tag will be "%s"', $newTag));

        // create new tag
        runCmd(sprintf("git tag %s", $newTag), $cwd);

        $pushCmd = sprintf("git push origin %s", $newTag);
        $question = new ConfirmationQuestion(
            sprintf('Push this new tag "%s"?', $pushCmd),
            true
        );
        if (!$helper->ask($input, $output, $question)) {
            $output->writeln(sprintf(
                'Skipping push of tag "%s" push tag by running "%s"',
                $newTag,
                $pushCmd
            ));
            return SingleCommandApplication::SUCCESS;
        }
        runCmd($pushCmd, $cwd);
        $output->writeln('New tag was pushed.',);
        return SingleCommandApplication::SUCCESS;
    })
    ->run();
