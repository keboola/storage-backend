<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\S3;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\CopyCommandCsvOptionsHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\SqlCommandBuilder;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;

class SnowflakeImportAdapter implements SnowflakeImportAdapterInterface
{
    private Connection $connection;

    private SqlCommandBuilder $sqlBuilder;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        $this->sqlBuilder = new SqlCommandBuilder();
    }

    public static function isSupported(Storage\SourceInterface $source, Storage\DestinationInterface $destination): bool
    {
        if (!$source instanceof Storage\S3\SourceFile) {
            return false;
        }
        if (!$destination instanceof Storage\Snowflake\Table) {
            return false;
        }

        return true;
    }

    /**
     * @param Storage\S3\SourceFile $source
     * @param Storage\Snowflake\Table $destination
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptionsInterface $importOptions,
        string $stagingTableName,
    ): int {
        $commands = $this->getCommands($source, $destination, $importOptions, $stagingTableName);

        foreach ($commands as $sql) {
            $this->connection->query($sql);
        }

        $rows = $this->connection->fetchAll($this->sqlBuilder->getTableItemsCountCommand(
            $destination->getSchema(),
            $stagingTableName,
        ));

        return (int) $rows[0]['count'];
    }

    /**
     * @return string[]
     */
    private function getCommands(
        Storage\S3\SourceFile $source,
        Storage\Snowflake\Table $destination,
        ImportOptionsInterface $importOptions,
        string $stagingTableName,
    ): array {
        $filesToImport = $source->getManifestEntries();
        $commands = [];
        $s3Prefix = $source->getS3Prefix() . '/';
        foreach (array_chunk($filesToImport, ImporterInterface::SLICED_FILES_CHUNK_SIZE) as $entries) {
            $quotedFiles = array_map(
                static function ($entry) use ($s3Prefix) {
                    return QuoteHelper::quote(strtr($entry, [$s3Prefix => '']));
                },
                $entries,
            );

            $commands[] = sprintf(
                'COPY INTO %s.%s
FROM %s 
CREDENTIALS = (AWS_KEY_ID = %s AWS_SECRET_KEY = %s)
REGION = %s
FILE_FORMAT = (TYPE=CSV %s)
FILES = (%s)',
                $this->connection->quoteIdentifier($destination->getSchema()),
                $this->connection->quoteIdentifier($stagingTableName),
                QuoteHelper::quote($source->getS3Prefix()),
                QuoteHelper::quote($source->getKey()),
                QuoteHelper::quote($source->getSecret()),
                QuoteHelper::quote($source->getRegion()),
                implode(' ', CopyCommandCsvOptionsHelper::getCsvCopyCommandOptions(
                    $importOptions,
                    $source->getCsvOptions(),
                )),
                implode(', ', $quotedFiles),
            );
        }
        return $commands;
    }
}
