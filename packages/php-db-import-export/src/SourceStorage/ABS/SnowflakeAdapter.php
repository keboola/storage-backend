<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\SourceStorage\ABS;

use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportAdapterInterface;
use Keboola\Db\ImportExport\SourceStorage\SourceInterface;

class SnowflakeAdapter implements SnowflakeImportAdapterInterface
{
    /**
     * @var Source
     */
    private $source;

    /**
     * @param Source $source
     */
    public function __construct(SourceInterface $source)
    {
        $this->source = $source;
    }

    /**
     * @inheritDoc
     */
    public function executeCopyCommands(
        array $commands,
        Connection $connection,
        ImportOptions $importOptions,
        ImportState $importState
    ): int {
        $timerName = sprintf('copyToStaging-%s', $this->source->getCsvFile()->getBasename());
        $importState->startTimer($timerName);
        $rowsCount = 0;
        foreach ($commands as $command) {
            $results = $connection->fetchAll($command);
            foreach ($results as $result) {
                $rowsCount += (int) $result['rows_loaded'];
            }
        }
        $importState->stopTimer($timerName);

        return $rowsCount;
    }

    public function getCopyCommands(
        ImportOptions $importOptions,
        string $stagingTableName
    ): array {
        $filesToImport = $this->source->getManifestEntries();
        $commands = [];
        foreach (array_chunk($filesToImport, ImporterInterface::SLICED_FILES_CHUNK_SIZE) as $entries) {
            $commands[] = sprintf(
                'COPY INTO %s.%s 
                FROM %s
                CREDENTIALS=(AZURE_SAS_TOKEN=\'%s\')
                FILE_FORMAT = (TYPE=CSV %s)
                FILES = (%s)',
                QuoteHelper::quoteIdentifier($importOptions->getSchema()),
                QuoteHelper::quoteIdentifier($stagingTableName),
                QuoteHelper::quote($this->source->getContainerUrl()),
                $this->source->getSasToken(),
                implode(' ', $this->getCsvCopyCommandOptions($importOptions, $this->source->getCsvFile())),
                implode(
                    ', ',
                    array_map(
                        function ($entry) {
                            return QuoteHelper::quote(strtr($entry, [$this->source->getContainerUrl() => '']));
                        },
                        $entries
                    )
                )
            );
        }
        return $commands;
    }

    private function getCsvCopyCommandOptions(
        ImportOptions $importOptions,
        CsvFile $csvFile
    ): array {
        $options = [
            sprintf('FIELD_DELIMITER = %s', QuoteHelper::quote($csvFile->getDelimiter())),
        ];

        if ($importOptions->getNumberOfIgnoredLines() > 0) {
            $options[] = sprintf('SKIP_HEADER = %d', $importOptions->getNumberOfIgnoredLines());
        }

        if ($csvFile->getEnclosure()) {
            $options[] = sprintf('FIELD_OPTIONALLY_ENCLOSED_BY = %s', QuoteHelper::quote($csvFile->getEnclosure()));
            $options[] = 'ESCAPE_UNENCLOSED_FIELD = NONE';
        } elseif ($csvFile->getEscapedBy()) {
            $options[] = sprintf('ESCAPE_UNENCLOSED_FIELD = %s', QuoteHelper::quote($csvFile->getEscapedBy()));
        }
        return $options;
    }
}
