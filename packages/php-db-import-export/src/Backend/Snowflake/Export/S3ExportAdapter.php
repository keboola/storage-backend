<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\Export;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface;
use Keboola\Db\ImportExport\ExportFileType;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\ExportOptionsInterface;
use Keboola\Db\ImportExport\Storage;

class S3ExportAdapter implements BackendExportAdapterInterface
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
        $timestampFormat = 'YYYY-MM-DD HH24:MI:SS';
        if (in_array(Exporter::FEATURE_FRACTIONAL_SECONDS, $exportOptions->features(), true)) {
            $timestampFormat = 'YYYY-MM-DD HH24:MI:SS.FF9';
        }
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
%s
)
ENCRYPTION = (
    TYPE = 'AWS_SSE_S3'
)
OVERWRITE = TRUE
MAX_FILE_SIZE=%d
DETAILED_OUTPUT = TRUE%s
EOT,
            $destination->getS3Prefix(),
            $destination->getFilePath(),
            $source->getFromStatement(),
            $destination->getKey(),
            $destination->getSecret(),
            $destination->getRegion(),
            FileFormat::getFileFormatForCopyInto($exportOptions),
            Exporter::DEFAULT_SLICE_SIZE,
            $exportOptions->getFileType() === ExportFileType::PARQUET ? PHP_EOL . 'HEADER=TRUE' : '',
        );

        $this->connection->executeQuery($sql, $source->getQueryBindings());

        /** @var array<array{FILE_NAME: string, FILE_SIZE: string, ROW_COUNT: string}> $unloadedFiles */
        $unloadedFiles = $this->connection->fetchAllAssociative('select * from table(result_scan(last_query_id()));');

        if ($exportOptions->generateManifest()) {
            (new Storage\S3\ManifestGenerator\S3SlicedManifestFromUnloadQueryResultGenerator($destination->getClient()))
                ->generateAndSaveManifest($destination->getRelativePath(), $unloadedFiles);
        }

        return $unloadedFiles;
    }
}
