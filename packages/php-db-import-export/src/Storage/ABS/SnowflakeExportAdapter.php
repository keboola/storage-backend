<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeExportAdapterInterface;
use Keboola\Db\ImportExport\ExportOptionsInterface;
use Keboola\Db\ImportExport\Storage;

/**
 * @deprecated use Keboola\Db\ImportExport\Backend\Snowflake\Export\AbsExportAdapter
 */
class SnowflakeExportAdapter implements SnowflakeExportAdapterInterface
{
    private Connection $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public static function isSupported(Storage\SourceInterface $source, Storage\DestinationInterface $destination): bool
    {
        if (!$source instanceof Storage\SqlSourceInterface) {
            return false;
        }
        if (!$destination instanceof Storage\ABS\DestinationFile) {
            return false;
        }
        return true;
    }

    /**
     * @param Storage\SqlSourceInterface $source
     * @param Storage\ABS\DestinationFile $destination
     * @return array<mixed>
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ExportOptionsInterface $exportOptions,
    ): array {
        $sql = sprintf(
            'COPY INTO \'%s%s\' 
FROM (%s)
CREDENTIALS=(AZURE_SAS_TOKEN=\'%s\')
FILE_FORMAT = (
    TYPE = \'CSV\'
    FIELD_DELIMITER = \',\'
    FIELD_OPTIONALLY_ENCLOSED_BY = \'\"\'
    %s
    TIMESTAMP_FORMAT = \'YYYY-MM-DD HH24:MI:SS\'
)
MAX_FILE_SIZE=200000000
DETAILED_OUTPUT = TRUE',
            $destination->getContainerUrl(BaseFile::PROTOCOL_AZURE),
            $destination->getFilePath(),
            $source->getFromStatement(),
            $destination->getSasToken(),
            $exportOptions->isCompressed() ? "COMPRESSION='GZIP'" : "COMPRESSION='NONE'",
        );

        $this->connection->query($sql, $source->getQueryBindings());

        /** @var array<array{FILE_NAME: string, FILE_SIZE: string, ROW_COUNT: string}> $unloadedFiles */
        $unloadedFiles = $this->connection->fetchAll('select * from table(result_scan(last_query_id()));');

        if ($exportOptions->generateManifest()) {
            (new Storage\ABS\ManifestGenerator\AbsSlicedManifestFromUnloadQueryResultGenerator(
                $destination->getClient(),
                $destination->getAccountName(),
            ))->generateAndSaveManifest($destination->getRelativePath(), $unloadedFiles);
        }

        return $unloadedFiles;
    }
}
