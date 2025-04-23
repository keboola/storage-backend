<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\S3;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeExportAdapterInterface;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\ExportOptionsInterface;
use Keboola\Db\ImportExport\Storage;

/**
 * @deprecated use Keboola\Db\ImportExport\Backend\Snowflake\Export\S3ExportAdapter
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
        if (!$destination instanceof Storage\S3\DestinationFile) {
            return false;
        }
        return true;
    }

    /**
     * @param Storage\SqlSourceInterface $source
     * @param Storage\S3\DestinationFile $destination
     * @param ExportOptions $exportOptions
     * @return array<mixed>
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ExportOptionsInterface $exportOptions,
    ): array {
        $sql = sprintf(
            <<<EOT
COPY INTO '%s/%s'
FROM (%s)
CREDENTIALS = (
    AWS_KEY_ID = '%s'
    AWS_SECRET_KEY = '%s'
)
REGION = '%s'
FILE_FORMAT = (
    TYPE = 'CSV' FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
    %s
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
)
ENCRYPTION = (
    TYPE = 'AWS_SSE_S3'
)
OVERWRITE = TRUE
MAX_FILE_SIZE=200000000
DETAILED_OUTPUT = TRUE
EOT,
            $destination->getS3Prefix(),
            $destination->getFilePath(),
            $source->getFromStatement(),
            $destination->getKey(),
            $destination->getSecret(),
            $destination->getRegion(),
            $exportOptions->isCompressed() ? "COMPRESSION='GZIP'" : "COMPRESSION='NONE'",
        );

        $unloadedFiles = $this->connection->fetchAll($sql, $source->getQueryBindings());

        if ($exportOptions->generateManifest()) {
            (new Storage\S3\ManifestGenerator\S3SlicedManifestFromUnloadQueryResultGenerator($destination->getClient()))
                ->generateAndSaveManifest($destination->getRelativePath(), $unloadedFiles);
        }

        return $unloadedFiles;
    }
}
