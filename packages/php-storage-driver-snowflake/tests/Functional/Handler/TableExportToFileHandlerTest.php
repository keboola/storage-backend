<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional\Handler;

use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\ImportExport\Storage\S3\DestinationFile;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Table\CreateProfileTableResponse;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileFormat;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FilePath;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileProvider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\S3Credentials;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Table\TableExportToFileCommand;
use Keboola\StorageDriver\Command\Table\TableExportToFileResponse;
use Keboola\StorageDriver\Snowflake\Handler\Table\TableExportToFileHandler;
use Keboola\StorageDriver\Snowflake\Tests\Functional\BaseCase;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;
use Tests\Keboola\Db\ImportExportCommon\StorageTrait;
use Tests\Keboola\Db\ImportExportCommon\StorageType;

final class TableExportToFileHandlerTest extends BaseCase
{

    protected const SCHEMA_NAME = 'workspaceTableExport';
    private const TABLE_NAME = 'table_to_file_test';

    public function testExport(): void
    {
        $credentials = $this->createCredentialsWithKeyPair();

        $handler = new TableExportToFileHandler();
        $command = (new TableExportToFileCommand());

        $path = new RepeatedField(GPBType::STRING);
        $path[] = self::SCHEMA_NAME;

        $command->setSource(
            (new Table())
                ->setPath($path)
                ->setTableName(self::TABLE_NAME),
        );

        $command->setFileProvider(FileProvider::S3);
        $command->setFileFormat(FileFormat::CSV);

        // Create DestinationFile (similar to what's used in import/export)
        $destination = new DestinationFile(
            (string) getenv('AWS_ACCESS_KEY_ID'),
            (string) getenv('AWS_SECRET_ACCESS_KEY'),
            (string) getenv('AWS_REGION'),
            (string) getenv('AWS_S3_BUCKET'),
            $this->getExportDir() . '/workspace_export_test',
        );

        // Now create command parts from destination (reverse of TableExportToFileHandler::createDestination)
        // Create S3Credentials from destination properties
        $s3Credentials = (new S3Credentials())
            ->setKey($destination->getKey())
            ->setSecret($destination->getSecret())
            ->setRegion($destination->getRegion());

        // Pack credentials into Any
        $fileCredentials = new Any();
        $fileCredentials->pack($s3Credentials);

        // Split filePath into path and fileName
        $filePath = $destination->getFilePath();
        $pathParts = explode('/', $filePath);
        $fileName = array_pop($pathParts);
        $pathWithoutFileName = implode('/', $pathParts);

        // Create FilePath
        $command->setFilePath(
            (new FilePath())
                ->setRoot($destination->getBucket())
                ->setPath($pathWithoutFileName)
                ->setFileName($fileName),
        );

        $command->setFileCredentials($fileCredentials)
            ->setExportOptions(
                (new ExportOptions())
                    ->setIsCompressed(false),
            );

        $response = $handler(
            $credentials,
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertInstanceOf(TableExportToFileResponse::class, $response);

    }

    protected function setUp(): void
    {
        $this->getSnowflakeConnection();

        $this->connection->executeQuery(sprintf(
            'CREATE OR REPLACE SCHEMA %s',
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
        ));

        $this->connection->executeQuery(
            sprintf(
                'DROP TABLE IF EXISTS %s.%s;',
                SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
                SnowflakeQuote::quoteSingleIdentifier(self::TABLE_NAME),
            ),
        );

        $this->connection->executeQuery(
            (new SnowflakeTableQueryBuilder())->getCreateTableCommand(
                self::SCHEMA_NAME,
                self::TABLE_NAME,
                new ColumnCollection([
                    new SnowflakeColumn(
                        'meal',
                        new Snowflake(Snowflake::TYPE_NUMBER),
                    ),
                    new SnowflakeColumn(
                        'description',
                        new Snowflake(Snowflake::TYPE_VARCHAR),
                    ),
                ]),
            ),
        );

        $this->connection->executeQuery(sprintf(
            // phpcs:disable
            <<<'SQL'
                INSERT INTO %s.%s ("meal", "description") VALUES
                (7, 'Spaghetti with meatballs'),
                (8, 'Chicken Alfredo'),
                (9, 'Caesar Salad'),
                (10, 'Grilled Salmon');
                SQL,
            // phpcs:enable
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_NAME),
        ));
    }

    protected function getDestinationSchema(): string
    {
        return self::SCHEMA_NAME;
    }

    protected function getSourceSchema(): string
    {
        return self::SCHEMA_NAME;
    }

    protected function getExportDir(): string
    {
        $buildPrefix = '';
        if (getenv('BUILD_PREFIX') !== false) {
            $buildPrefix = getenv('BUILD_PREFIX');
        }

        return $this->getExportBlobDir()
            . '-'
            . $buildPrefix
            . '-'
            . getenv('SUITE');
    }

    protected function getExportBlobDir(): string
    {
        $path = '';
        switch (getenv('STORAGE_TYPE')) {
//            case StorageType::STORAGE_S3:
            case 'S3':
                $key = getenv('AWS_S3_KEY');
                if ($key) {
                    $path = $key . '/';
                }
        }

        return $path . 'test_export';
    }
}
