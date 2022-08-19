<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\Synapse\PolyBaseCommandBuilder;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseExportAdapterInterface;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseExportOptions;
use Keboola\Db\ImportExport\ExportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Throwable;

class SynapseExportAdapter implements SynapseExportAdapterInterface
{
    private Connection $connection;

    private PolyBaseCommandBuilder $polyBase;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        $this->polyBase = new PolyBaseCommandBuilder();
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
     * @param SynapseExportOptions $exportOptions
     * @return array<mixed>
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ExportOptionsInterface $exportOptions
    ): array {
        $compression = $exportOptions->isCompressed() ?
            ',DATA_COMPRESSION = \'org.apache.hadoop.io.compress.GzipCodec\'' :
            '';
        //https://docs.microsoft.com/en-us/sql/t-sql/statements/create-external-file-format-transact-sql?view=azure-sqldw-latest#arguments
        $dateFormat = 'yyyy-MM-dd HH:mm:ss';
        $exportId = $exportOptions->getExportId();
        $blobMasterKey = $destination->getBlobMasterKey();
        $containerUrl = $destination->getPolyBaseUrl($exportOptions->getExportCredentialsType());
        $credentialsId = $exportId . '_StorageCredential';
        $dataSourceId = $exportId . '_StorageSource';
        $fileFormatId = $exportId . '_StorageFileFormat';
        $tableId = $exportId . '_StorageExternalTable';
        $exportPath = $destination->getFilePath();

        $exception = null;
        try {
            $sql = $this->polyBase->getCredentialsQuery(
                $credentialsId,
                $exportOptions->getExportCredentialsType(),
                $blobMasterKey
            );
            $this->connection->exec($sql);

            $sql = $this->polyBase->getDataSourceQuery($dataSourceId, $containerUrl, $credentialsId);
            $this->connection->exec($sql);

            $sql = $this->polyBase->getFileFormatQuery($fileFormatId, $dateFormat, $compression);
            $this->connection->exec($sql);

            $sql = $this->polyBase->getExternalTableQuery($source, $tableId, $exportPath, $dataSourceId, $fileFormatId);

            $dataTypes = [];
            if ($source instanceof Storage\Synapse\SelectSource) {
                $dataTypes = $source->getDataTypes();
            }

            $this->connection->executeQuery($sql, $source->getQueryBindings(), $dataTypes);
        } catch (Throwable $e) {
            //exception is saved for later while we try to clean created resources
            $exception = $e;
        }

        foreach ($this->polyBase->getPolyBaseCleanUpQueries(
            $fileFormatId,
            $dataSourceId,
            $credentialsId,
            $tableId
        ) as $sql
        ) {
            try {
                $this->connection->exec($sql);
            } catch (Throwable $e) {
                // we want to perform whole clean up
            }
        }

        if ($exception !== null) {
            throw $exception;
        }

        (new Storage\ABS\ManifestGenerator\AbsSlicedManifestFromFolderGenerator($destination->getClient()))
            ->generateAndSaveManifest($destination->getRelativePath());

        return [];
    }
}
