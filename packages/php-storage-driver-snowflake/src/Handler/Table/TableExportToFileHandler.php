<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Handler\Table;

use Google\Protobuf\Internal\Message;
use Keboola\Db\ImportExport\Backend\Snowflake\Export\Exporter;
use Keboola\Db\ImportExport\ExportFileType;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ABSCredentials;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileFormat;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileProvider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\GCSCredentials;
use Keboola\StorageDriver\Command\Table\ImportExportShared\S3Credentials;
use Keboola\StorageDriver\Command\Table\TableExportToFileCommand;
use Keboola\StorageDriver\Command\Table\TableExportToFileResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\StorageDriver\Snowflake\ConnectionFactory;
use Throwable;

final class TableExportToFileHandler extends BaseHandler
{
    /**
     * @param array<string, string> $features
     * @throws TableExportFailedException
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): Message|null {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof TableExportToFileCommand);

        $this->validateCommand($command);

        $connection = ConnectionFactory::createFromCredentials($credentials);

        $source = $this->createSource($command);
        $destination = $this->createDestination($command);
        $exportOptions = $this->createExportOptions($command, $features);

        try {
            $exporter = new Exporter($connection);
            $exporter->exportTable($source, $destination, $exportOptions);
        } catch (Throwable $e) {
            throw new TableExportFailedException($e->getMessage(), $e->getCode(), $e);
        }

        $response = new TableExportToFileResponse();
        $tableInfo = new TableInfo();
        $tableInfo->setPath($command->getSource()->getPath());
        $tableInfo->setTableName($command->getSource()->getTableName());
        $response->setTableInfo($tableInfo);

        return $response;
    }

    /**
     * @phpstan-assert !null $command->getSource()
     * @phpstan-assert !null $command->getSource()->getTableName()
     * @phpstan-assert !null $command->getFilePath()
     * @phpstan-assert !null $command->getFileCredentials()
     */
    private function validateCommand(TableExportToFileCommand $command): void
    {
        if (!$command->getSource()) {
            throw new Exception('Source table is required');
        }

        if (!$command->getSource()->getTableName()) {
            throw new Exception('Source table name is required');
        }

        if (!$command->getFilePath()) {
            throw new Exception('File path is required');
        }

        if (!$command->getFileCredentials()) {
            throw new Exception('File credentials are required');
        }
    }

    private function createSource(TableExportToFileCommand $command): Storage\Snowflake\Table
    {
        $source = $command->getSource();
        assert($source !== null);

        $sourcePath = $source->getPath();
        // taking the last element of path as schema, because there might be also database in path
        assert($sourcePath->count() > 0, 'Source path must have at least one element for schema name');
        /** @var string $schema */
        $schema = $sourcePath->offsetGet($sourcePath->count() - 1);
        $tableName = $source->getTableName();

        $columns = [];
        $exportOptions = $command->getExportOptions();
        if ($exportOptions !== null) {
            $columns = ProtobufHelper::repeatedStringToArray($exportOptions->getColumnsToExport());
        }

        return new Storage\Snowflake\Table($schema, $tableName, $columns);
    }

    private function createDestination(TableExportToFileCommand $command): Storage\DestinationInterface
    {
        $filePath = $command->getFilePath();
        assert($filePath !== null);

        $fileProvider = $command->getFileProvider();
        $fileCredentials = $command->getFileCredentials();
        assert($fileCredentials !== null);

        $credentials = $fileCredentials->unpack();
        assert($credentials !== null);

        $pathComponents = [];
        if ($filePath->getPath()) {
            $pathComponents[] = $filePath->getPath();
        }
        if ($filePath->getFileName()) {
            $pathComponents[] = $filePath->getFileName();
        }
        $fullPath = implode('/', $pathComponents);

        switch (true) {
            case $fileProvider === FileProvider::S3 && $credentials instanceof S3Credentials:
                return new Storage\S3\DestinationFile(
                    $credentials->getKey(),
                    $credentials->getSecret(),
                    $credentials->getRegion(),
                    $filePath->getRoot(),
                    $fullPath,
                );

            case $fileProvider === FileProvider::ABS && $credentials instanceof ABSCredentials:
                return new Storage\ABS\DestinationFile(
                    $filePath->getRoot(),
                    $fullPath,
                    $credentials->getSasToken(),
                    $credentials->getAccountName(),
                );

            case $fileProvider === FileProvider::GCS && $credentials instanceof GCSCredentials:
                /**
                 * @var array{
                 * type: string,
                 * project_id: string,
                 * private_key_id: string,
                 * private_key: string,
                 * client_email: string,
                 * client_id: string,
                 * auth_uri: string,
                 * token_uri: string,
                 * auth_provider_x509_cert_url: string,
                 * client_x509_cert_url: string,
                 * } $credentialsArray
                 */
                $credentialsArray = json_decode($credentials->getKey(), true);

                $credentialsArray['private_key'] = $credentials->getSecret();
                return new Storage\GCS\DestinationFile(
                    $filePath->getRoot(),
                    $fullPath,
                    $credentials->getSecurityIntegrationName(),
                    $credentialsArray,
                );

            default:
                throw new Exception(sprintf('Unsupported file provider: "%s"', $fileProvider));
        }
    }

    /**
     * @param string[] $features
     */
    private function createExportOptions(
        TableExportToFileCommand $command,
        array $features,
    ): ExportOptions {
        $isCompressed = false;

        $exportOptions = $command->getExportOptions();
        if ($exportOptions !== null) {
            $isCompressed = $exportOptions->getIsCompressed();
        }

        $fileFormat = $command->getFileFormat();
        $fileType = match ($fileFormat) {
            FileFormat::CSV => ExportFileType::CSV,
            FileFormat::PARQUET => ExportFileType::PARQUET,
            default => throw new Exception(sprintf('Unsupported file format: "%s"', $fileFormat)),
        };

        return new ExportOptions(
            isCompressed: $isCompressed,
            generateManifest: ExportOptions::MANIFEST_AUTOGENERATED,
            features: $features,
            fileType: $fileType,
        );
    }
}
