<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\Export;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface;
use Keboola\Db\ImportExport\ExportOptionsInterface;
use Keboola\Db\ImportExport\Storage;

class AbsExportAdapter implements BackendExportAdapterInterface
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
        $timestampFormat = 'YYYY-MM-DD HH24:MI:SS';
        if (in_array(Exporter::FEATURE_FRACTIONAL_SECONDS, $exportOptions->features(), true)) {
            $timestampFormat = 'YYYY-MM-DD HH24:MI:SS.FF9';
        }
        $sql = sprintf(
            'COPY INTO \'%s%s\' 
FROM (%s)
CREDENTIALS=(AZURE_SAS_TOKEN=\'%s\')
FILE_FORMAT = (
    TYPE = \'CSV\'
    FIELD_DELIMITER = \',\'
    FIELD_OPTIONALLY_ENCLOSED_BY = \'\"\'
    %s
    TIMESTAMP_FORMAT = \'%s\',
    NULL_IF = ()
)
MAX_FILE_SIZE=50000000
DETAILED_OUTPUT = TRUE',
            $destination->getContainerUrl(Storage\ABS\BaseFile::PROTOCOL_AZURE),
            $destination->getFilePath(),
            $source->getFromStatement(),
            $destination->getSasToken(),
            $exportOptions->isCompressed() ? "COMPRESSION='GZIP'" : "COMPRESSION='NONE'",
            $timestampFormat,
        );

        /** @var array<array{FILE_NAME: string, FILE_SIZE: string, ROW_COUNT: string}> $unloadedFiles */
        $unloadedFiles = $this->connection->fetchAllAssociative($sql, $source->getQueryBindings());

        if ($exportOptions->generateManifest()) {
            (new Storage\ABS\ManifestGenerator\AbsSlicedManifestFromUnloadQueryResultGenerator(
                $destination->getClient(),
                $destination->getAccountName(),
            ))
                ->generateAndSaveManifest($destination->getRelativePath(), $unloadedFiles);
        }

        return $unloadedFiles;
    }
}
