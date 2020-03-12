<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;

class SynapseExportAdapter implements BackendExportAdapterInterface
{
    /**
     * @var Storage\ABS\DestinationFile
     */
    private $destination;

    /**
     * @param Storage\ABS\DestinationFile $destination
     */
    public function __construct(Storage\DestinationInterface $destination)
    {
        $this->destination = $destination;
    }

    /**
     * phpcs:disable SlevomatCodingStandard.TypeHints.TypeHintDeclaration.MissingParameterTypeHint
     *
     * @param Storage\SqlSourceInterface $source
     * @param Connection $connection
     * @throws \Exception
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        ExportOptions $exportOptions,
        $connection = null
    ): void {
        if (!$source instanceof Storage\SqlSourceInterface) {
            throw new \Exception(sprintf(
                'Source "%s" must implement "%s".',
                get_class($source),
                Storage\SqlSourceInterface::class
            ));
        }

        if ($connection === null || !$connection instanceof Connection) {
            throw new \Exception(sprintf('Connection must be instance of "%s"', Connection::class));
        }

        /** @var SQLServer2012Platform $platform */
        $platform = $connection->getDatabasePlatform();
        $compression = $exportOptions->isCompresed() ?
            ',DATA_COMPRESSION = \'org.apache.hadoop.io.compress.GzipCodec\'' :
            '';
        //https://docs.microsoft.com/en-us/sql/t-sql/statements/create-external-file-format-transact-sql?view=azure-sqldw-latest#arguments
        $dateFormat = 'yyyy-MM-dd HH:mm:ss';
        $exportId = $exportOptions->getExportId();
// TODO: this should work with sas in future
//        $sasToken = $connection->quote(urldecode($this->destination->getSasToken()));
        $sasToken = $connection->quote($this->destination->getBlobMasterKey());
        $containerUrl = $connection->quote($this->destination->getPolyBaseUrl());
        $credentialsId = $platform->quoteSingleIdentifier($exportId . '_StorageCredential');
        $dataSourceId = $platform->quoteSingleIdentifier($exportId . '_StorageSource');
        $fileFormatId = $platform->quoteSingleIdentifier($exportId . '_StorageFileFormat');
        $tableId = $platform->quoteSingleIdentifier($exportId . '_StorageExternalTable');
        $exportPath = $this->destination->getFilePath();

        $exception = null;
        try {
            $sql = $this->getCredentialsQuery($credentialsId, $sasToken);
            $connection->exec($sql);

            $sql = $this->getDataSourceQuery($dataSourceId, $containerUrl, $credentialsId);
            $connection->exec($sql);

            $sql = $this->getFileFormatQuery($fileFormatId, $dateFormat, $compression);
            $connection->exec($sql);

            $sql = $this->getExternalTableQuery($source, $tableId, $exportPath, $dataSourceId, $fileFormatId);

            $dataTypes = [];
            if ($source instanceof Storage\Synapse\SelectSource) {
                $dataTypes = $source->getDataTypes();
            }

            $connection->executeQuery($sql, $source->getQueryBindings(), $dataTypes);
        } catch (\Throwable $e) {
            //exception is saved for later while we try to clean created resources
            $exception = $e;
        }

        // clean up
        try {
            $connection->exec(sprintf('DROP EXTERNAL TABLE %s', $tableId));
        } catch (\Throwable $e) {
            // we want to perform whole clean up
        }
        try {
            $connection->exec(sprintf('DROP EXTERNAL FILE FORMAT %s', $fileFormatId));
        } catch (\Throwable $e) {
            // we want to perform whole clean up
        }
        try {
            $connection->exec(sprintf('DROP EXTERNAL DATA SOURCE %s', $dataSourceId));
        } catch (\Throwable $e) {
            // we want to perform whole clean up
        }
        try {
            $connection->exec(sprintf('DROP DATABASE SCOPED CREDENTIAL %s', $credentialsId));
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
