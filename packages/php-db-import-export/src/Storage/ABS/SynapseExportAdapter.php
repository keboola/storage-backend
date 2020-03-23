<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseExportAdapterInterface;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;

class SynapseExportAdapter implements SynapseExportAdapterInterface
{
    /** @var Connection */
    private $connection;

    /** @var AbstractPlatform|SQLServer2012Platform */
    private $platform;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        $this->platform = $connection->getDatabasePlatform();
    }

    public static function isSupported(Storage\SourceInterface $source, Storage\DestinationInterface $destination): bool
    {
        if (!$source instanceof Storage\ABS\DestinationFile) {
            return false;
        }
        if (!$destination instanceof Storage\SqlSourceInterface) {
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
        $sasToken = $this->connection->quote($destination->getBlobMasterKey());
        $containerUrl = $this->connection->quote($destination->getPolyBaseUrl());
        $credentialsId = $this->platform->quoteSingleIdentifier($exportId . '_StorageCredential');
        $dataSourceId = $this->platform->quoteSingleIdentifier($exportId . '_StorageSource');
        $fileFormatId = $this->platform->quoteSingleIdentifier($exportId . '_StorageFileFormat');
        $tableId = $this->platform->quoteSingleIdentifier($exportId . '_StorageExternalTable');
        $exportPath = $destination->getFilePath();

        $exception = null;
        try {
            $sql = $this->getCredentialsQuery($credentialsId, $sasToken);
            $this->connection->exec($sql);

            $sql = $this->getDataSourceQuery($dataSourceId, $containerUrl, $credentialsId);
            $this->connection->exec($sql);

            $sql = $this->getFileFormatQuery($fileFormatId, $dateFormat, $compression);
            $this->connection->exec($sql);

            $sql = $this->getExternalTableQuery($source, $tableId, $exportPath, $dataSourceId, $fileFormatId);

            $dataTypes = [];
            if ($source instanceof Storage\Synapse\SelectSource) {
                $dataTypes = $source->getDataTypes();
            }

            $this->connection->executeQuery($sql, $source->getQueryBindings(), $dataTypes);
        } catch (\Throwable $e) {
            //exception is saved for later while we try to clean created resources
            $exception = $e;
        }

        // clean up
        try {
            $this->connection->exec(sprintf('DROP EXTERNAL TABLE %s', $tableId));
        } catch (\Throwable $e) {
            // we want to perform whole clean up
        }
        try {
            $this->connection->exec(sprintf('DROP EXTERNAL FILE FORMAT %s', $fileFormatId));
        } catch (\Throwable $e) {
            // we want to perform whole clean up
        }
        try {
            $this->connection->exec(sprintf('DROP EXTERNAL DATA SOURCE %s', $dataSourceId));
        } catch (\Throwable $e) {
            // we want to perform whole clean up
        }
        try {
            $this->connection->exec(sprintf('DROP DATABASE SCOPED CREDENTIAL %s', $credentialsId));
        } catch (\Throwable $e) {
            // we want to perform whole clean up
        }

        if ($exception !== null) {
            throw $exception;
        }
    }

    private function getCredentialsQuery(string $credentialsId, string $sasToken): string
    {
// TODO: this should work with SAS token in future IDENTITY must be than SHARED ACCESS SIGNATURE
        return <<< EOT
CREATE DATABASE SCOPED CREDENTIAL $credentialsId
WITH
    IDENTITY = 'user',
    SECRET = $sasToken
;
EOT;
    }

    private function getDataSourceQuery(string $dataSourceId, string $containerUrl, string $credentialsId): string
    {
        return <<<EOT
CREATE EXTERNAL DATA SOURCE $dataSourceId
WITH 
(
    TYPE = HADOOP,
    LOCATION = $containerUrl,
    CREDENTIAL = $credentialsId
);
EOT;
    }

    private function getFileFormatQuery(string $fileFormatId, string $dateFormat, string $compression): string
    {
        return <<<EOT
CREATE EXTERNAL FILE FORMAT $fileFormatId
WITH
(
    FORMAT_TYPE = DelimitedText,
    FORMAT_OPTIONS 
    (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        DATE_FORMAT = '$dateFormat',
        USE_TYPE_DEFAULT = FALSE
    )
    $compression
);
EOT;
    }

    private function getExternalTableQuery(
        Storage\SqlSourceInterface $source,
        string $tableId,
        string $exportPath,
        string $dataSourceId,
        string $fileFormatId
    ): string {
        $from = $source->getFromStatement();

        return <<<EOT
CREATE EXTERNAL TABLE $tableId
WITH 
(
    LOCATION='$exportPath',
    DATA_SOURCE = $dataSourceId,
    FILE_FORMAT = $fileFormatId
)
AS
$from
EOT;
    }
}
