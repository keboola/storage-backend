<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportAdapterInterface;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use Keboola\Db\ImportExport\Storage;

class SnowflakeImportAdapter implements SnowflakeImportAdapterInterface
{
    /** @var Connection */
    private $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public static function isSupported(Storage\SourceInterface $source, Storage\DestinationInterface $destination): bool
    {
        if (!$source instanceof Storage\ABS\SourceFile) {
            return false;
        }
        if (!$destination instanceof Storage\Snowflake\Table) {
            return false;
        }
        return true;
    }

    /**
     * @param Storage\ABS\SourceFile $source
     * @param Storage\Snowflake\Table $destination
     */
    public function getCopyCommands(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptions $importOptions,
        string $stagingTableName
    ): array {
        $filesToImport = $source->getManifestEntries();
        $commands = [];
        foreach (array_chunk($filesToImport, ImporterInterface::SLICED_FILES_CHUNK_SIZE) as $entries) {
            $quotedFiles = array_map(
                static function ($entry) use ($source) {
                    return QuoteHelper::quote(
                        strtr(
                            $entry,
                            [$source->getContainerUrl(BaseFile::PROTOCOL_AZURE) => '']
                        )
                    );
                },
                $entries
            );

            $commands[] = sprintf(
                'COPY INTO %s.%s 
FROM %s
CREDENTIALS=(AZURE_SAS_TOKEN=\'%s\')
FILE_FORMAT = (TYPE=CSV %s)
FILES = (%s)',
                $this->connection->quoteIdentifier($destination->getSchema()),
                $this->connection->quoteIdentifier($stagingTableName),
                QuoteHelper::quote($source->getContainerUrl(BaseFile::PROTOCOL_AZURE)),
                $source->getSasToken(),
                implode(' ', $this->getCsvCopyCommandOptions($importOptions, $source->getCsvOptions())),
                implode(', ', $quotedFiles)
            );
        }
        return $commands;
    }

    private function getCsvCopyCommandOptions(
        ImportOptions $importOptions,
        CsvOptions $csvOptions
    ): array {
        $options = [
            sprintf('FIELD_DELIMITER = %s', QuoteHelper::quote($csvOptions->getDelimiter())),
        ];

        if ($importOptions->getNumberOfIgnoredLines() > 0) {
            $options[] = sprintf('SKIP_HEADER = %d', $importOptions->getNumberOfIgnoredLines());
        }

        if ($csvOptions->getEnclosure()) {
            $options[] = sprintf('FIELD_OPTIONALLY_ENCLOSED_BY = %s', QuoteHelper::quote($csvOptions->getEnclosure()));
            $options[] = 'ESCAPE_UNENCLOSED_FIELD = NONE';
        } elseif ($csvOptions->getEscapedBy()) {
            $options[] = sprintf('ESCAPE_UNENCLOSED_FIELD = %s', QuoteHelper::quote($csvOptions->getEscapedBy()));
        }
        return $options;
    }
}
