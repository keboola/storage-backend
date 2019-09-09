<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\CommandBuilder;

use function GuzzleHttp\Psr7\str;
use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\File;
use Keboola\Db\ImportExport\ImportOptions;

class AbsBuilder
{
    private const SLICED_FILES_CHUNK_SIZE = 1000;

    /** @var Connection */
    private $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public function buildFileCopyCommands(ImportOptions $importOptions, File\Azure $file): array
    {
        $filesToImport = $file->getManifestEntries();
        $commands = [];
        foreach (array_chunk($filesToImport, self::SLICED_FILES_CHUNK_SIZE) as $entries) {
            $commands[] = sprintf(
                'COPY INTO %s.%s 
                FROM %s
                CREDENTIALS=(AZURE_SAS_TOKEN=\'%s\')
                FILE_FORMAT = (TYPE=CSV %s )
                FILES = (%s)',
                $this->connection->quoteIdentifier($importOptions->getSchema()),
                $this->connection->quoteIdentifier($importOptions->getTableName()),
                $this->quote($file->getContainerUrl()),
                $file->getSasToken(),
                implode(' ', $this->buildCsvCopyCommandOptions($importOptions, $file->getCsvFile())),
                implode(
                    ', ',
                    array_map(
                        function ($entry) use ($file) {
                            return $this->quote(strtr($entry, [$file->getContainerUrl() => '']));
                        },
                        $entries
                    )
                )
            );
        }
        return $commands;
    }

    private function buildCsvCopyCommandOptions(ImportOptions $importOptions, CsvFile $csvFile): array
    {
        $options = [
            sprintf('FIELD_DELIMITER = %s', $this->quote($csvFile->getDelimiter())),
        ];

        if ($importOptions->getNumberOfIgnoredLines() > 0) {
            $options[] = sprintf('SKIP_HEADER = %d', $importOptions->getNumberOfIgnoredLines());
        }

        if ($csvFile->getEnclosure()) {
            $options[] = sprintf('FIELD_OPTIONALLY_ENCLOSED_BY = %s', $this->quote($csvFile->getEnclosure()));
            $options[] = 'ESCAPE_UNENCLOSED_FIELD = NONE';
        } elseif ($csvFile->getEscapedBy()) {
            $options[] = sprintf('ESCAPED_UNENCLOSED_FIELD = %s', $this->quote($csvFile->getEscapedBy()));
        }
        return $options;
    }

    private function quote(string $value): string
    {
        return "'" . addslashes($value) . "'";
    }
}
