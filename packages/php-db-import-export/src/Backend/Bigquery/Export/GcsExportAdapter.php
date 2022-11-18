<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\Export;

use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\ExportOptionsInterface;
use Keboola\Db\ImportExport\Storage;

class GcsExportAdapter implements BackendExportAdapterInterface
{
    private BigQueryClient $bqClient;

    public function __construct(BigQueryClient $bqClient)
    {
        $this->bqClient = $bqClient;
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
        ExportOptionsInterface $exportOptions
    ): array {
        $sql = sprintf(
            'EXPORT DATA
OPTIONS (
    uri = \'gs://%s*.csv%s\'
    ,format = \'CSV\'
    ,overwrite = true
    ,header = false
    ,field_delimiter = \'%s\'
    %s
) AS (
    %s
);',
            $destination->getRelativePath()->getPathname(),
            $exportOptions->isCompressed() ? '.gz' : '', // add file suffix if gzip
            CsvOptions::DEFAULT_DELIMITER,
            $exportOptions->isCompressed() ? ',compression=\'GZIP\'' : '',
            $source->getFromStatement(),
        );
        $this->bqClient->runQuery(
            $this->bqClient->query($sql)
        );

        if ($exportOptions->generateManifest()) {
            (new Storage\GCS\ManifestGenerator\GcsSlicedManifestFromFolderGenerator(
                $destination->getClient()
            ))
                ->generateAndSaveManifest($destination->getRelativePath());
        }

        return [];
    }
}
