<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\Synapse\PolyBaseCommandBuilder;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseExportAdapterInterface;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;

class SynapseExportAdapter implements SynapseExportAdapterInterface
{
    /** @var Connection */
    private $connection;

    /** @var PolyBaseCommandBuilder */
    private $polyBase;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        $this->polyBase = new PolyBaseCommandBuilder($connection);
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
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ExportOptions $exportOptions
    ): void {
        $compression = $exportOptions->isCompresed() ?
            ',DATA_COMPRESSION = \'org.apache.hadoop.io.compress.GzipCodec\'' :
            '';
        //https://docs.microsoft.com/en-us/sql/t-sql/statements/create-external-file-format-transact-sql?view=azure-sqldw-latest#arguments
        $dateFormat = 'yyyy-MM-dd HH:mm:ss';
        $exportId = $exportOptions->getExportId();
        $sasToken = $destination->getBlobMasterKey();
        $containerUrl = $destination->getPolyBaseUrl();
        $credentialsId = $exportId . '_StorageCredential';
        $dataSourceId = $exportId . '_StorageSource';
        $fileFormatId = $exportId . '_StorageFileFormat';
        $tableId = $exportId . '_StorageExternalTable';
        $exportPath = $destination->getFilePath();

        $exception = null;
        try {
            $sql = $this->polyBase->getCredentialsQuery($credentialsId, $sasToken);
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
        } catch (\Throwable $e) {
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
            } catch (\Throwable $e) {
                // we want to perform whole clean up
            }
        }

        if ($exception !== null) {
            throw $exception;
        }
    }
}
