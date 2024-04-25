<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\GCS;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeExportAdapterInterface;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\ExportOptionsInterface;
use Keboola\Db\ImportExport\Storage;

/**
 * @deprecated use Keboola\Db\ImportExport\Backend\Snowflake\Export\GcsExportAdapter
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
        if (!$destination instanceof Storage\GCS\DestinationFile) {
            return false;
        }
        return true;
    }

    /**
     * @param Storage\SqlSourceInterface $source
     * @param Storage\GCS\DestinationFile $destination
     * @param ExportOptions $exportOptions
     * @return array<mixed>
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ExportOptionsInterface $exportOptions,
    ): array {
        $sql = sprintf(
            'COPY INTO \'%s/%s\'
FROM (%s)
STORAGE_INTEGRATION = "%s"
FILE_FORMAT = (
    TYPE = \'CSV\'
    FIELD_DELIMITER = \',\'
    FIELD_OPTIONALLY_ENCLOSED_BY = \'\"\'
    %s
    TIMESTAMP_FORMAT = \'YYYY-MM-DD HH24:MI:SS\'
)
MAX_FILE_SIZE=50000000
DETAILED_OUTPUT = TRUE',
            $destination->getGcsPrefix(),
            $destination->getFilePath(),
            $source->getFromStatement(),
            $destination->getStorageIntegrationName(),
            $exportOptions->isCompressed() ? "COMPRESSION='GZIP'" : "COMPRESSION='NONE'",
        );

        $unloadedFiles = $this->connection->fetchAll($sql, $source->getQueryBindings());

        if ($exportOptions->generateManifest()) {
            (new Storage\GCS\ManifestGenerator\GcsSlicedManifestFromUnloadQueryResultGenerator(
                $destination->getClient(),
                new Storage\GCS\ManifestGenerator\WriteStreamFactory(),
            ))
                ->generateAndSaveManifest($destination->getRelativePath(), $unloadedFiles);
        }

        return $unloadedFiles;
    }
}
