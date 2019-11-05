<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExport;

use DateTime;
use Keboola\Csv\CsvFile;
use MicrosoftAzure\Storage\Blob\BlobSharedAccessSignatureHelper;
use MicrosoftAzure\Storage\Common\Internal\Resources;
use PHPUnit\Framework\TestCase;
use Keboola\Db\ImportExport\SourceStorage;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;

abstract class ImportExportBaseTest extends TestCase
{
    protected const DATA_DIR = __DIR__ . '/data/';

    /**
     * @param int|string $sortKey
     */
    protected function assertArrayEqualsSorted(
        array $expected,
        array $actual,
        $sortKey,
        string $message = ''
    ): void {
        $comparsion = function ($attrLeft, $attrRight) use ($sortKey) {
            if ($attrLeft[$sortKey] === $attrRight[$sortKey]) {
                return 0;
            }
            return $attrLeft[$sortKey] < $attrRight[$sortKey] ? -1 : 1;
        };
        usort($expected, $comparsion);
        usort($actual, $comparsion);
        $this->assertEquals($expected, $actual, $message);
    }

    protected function createABSSourceInstanceFromCsv(
        CsvFile $file,
        bool $isSliced = false
    ): SourceStorage\ABS\Source {
        return new SourceStorage\ABS\Source(
            (string) getenv('ABS_CONTAINER_NAME'),
            $file->getFilename(),
            $this->getCredentialsForAzureContainer((string) getenv('ABS_CONTAINER_NAME')),
            (string) getenv('ABS_ACCOUNT_NAME'),
            $file,
            $isSliced
        );
    }

    protected function getCredentialsForAzureContainer(
        string $container
    ): string {
        $sasHelper = new BlobSharedAccessSignatureHelper(
            (string) getenv('ABS_ACCOUNT_NAME'),
            (string) getenv('ABS_ACCOUNT_KEY')
        );
        $expirationDate = (new DateTime())->modify('+1hour');
        return $sasHelper->generateBlobServiceSharedAccessSignatureToken(
            Resources::RESOURCE_TYPE_CONTAINER,
            $container,
            'rwl',
            $expirationDate,
            (new DateTime())
        );
    }

    protected function createABSSourceInstance(
        string $file,
        bool $isSliced = false
    ): SourceStorage\ABS\Source {
        return new SourceStorage\ABS\Source(
            (string) getenv('ABS_CONTAINER_NAME'),
            $file,
            $this->getCredentialsForAzureContainer((string) getenv('ABS_CONTAINER_NAME')),
            (string) getenv('ABS_ACCOUNT_NAME'),
            new CsvFile($file), //TODO: create file inside or use only CSV file
            $isSliced
        );
    }

    private function getCsvFilesForManifest(string $manifest): array
    {
        $path = (new SplFileInfo(self::DATA_DIR . $manifest, '', ''))->getPath();
        $files = (new Finder)->in($path)->files()->depth(0)->name('/^((?!.csvmanifest).)*$/');

        $filesContent = [];
        $filesHeader = [];
        /** @var SplFileInfo $file */
        foreach ($files as $file) {
            $csvFile = new CsvFile($file);
            $csvFileRows = [];
            foreach ($csvFile as $row) {
                $csvFileRows[] = $row;
            }

            if (empty($filesHeader)) {
                $filesHeader = array_shift($csvFileRows);
            } else {
                $this->assertSame(
                    $filesHeader,
                    array_shift($csvFileRows),
                    'Provided files have incosistent headers'
                );
            }
            foreach ($csvFileRows as $fileRow) {
                $filesContent[] = $fileRow;
            }
        }

        return [$filesHeader, $filesContent];
    }
}
