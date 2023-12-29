<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\ToStage;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\CopyCommandCsvOptionsHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeException;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\ABS\BaseFile;
use Keboola\Db\ImportExport\Storage\ABS\SourceFile;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Throwable;

class FromABSCopyIntoAdapter implements CopyAdapterInterface
{
    private Connection $connection;

    private const SLICED_FILES_CHUNK_SIZE = 1000;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * @param SourceFile $source
     * @param SnowflakeTableDefinition $destination
     * @param SnowflakeImportOptions $importOptions
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        TableDefinitionInterface $destination,
        ImportOptionsInterface $importOptions,
    ): int {
        try {
            $files = $source->getManifestEntries();
            foreach (array_chunk($files, self::SLICED_FILES_CHUNK_SIZE) as $files) {
                $cmd = $this->getCopyCommand(
                    $source,
                    $destination,
                    $importOptions,
                    $files,
                );
                $this->connection->executeStatement(
                    $cmd,
                );
            }
        } catch (Throwable $e) {
            throw SnowflakeException::covertException($e);
        }

        $ref = new SnowflakeTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName(),
        );

        return $ref->getRowsCount();
    }

    /**
     * @param string[] $files
     */
    private function getCopyCommand(
        Storage\ABS\SourceFile $source,
        SnowflakeTableDefinition $destination,
        SnowflakeImportOptions $importOptions,
        array $files,
    ): string {
        $quotedFiles = array_map(
            static function ($entry) use ($source) {
                return QuoteHelper::quote(
                    strtr(
                        $entry,
                        [$source->getContainerUrl(BaseFile::PROTOCOL_AZURE) => ''],
                    ),
                );
            },
            $files,
        );

        return sprintf(
            'COPY INTO %s.%s 
FROM %s
CREDENTIALS=(AZURE_SAS_TOKEN=\'%s\')
FILE_FORMAT = (TYPE=CSV %s, NULL_IF=(\'\'))
FILES = (%s)',
            SnowflakeQuote::quoteSingleIdentifier($destination->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($destination->getTableName()),
            QuoteHelper::quote($source->getContainerUrl(BaseFile::PROTOCOL_AZURE)),
            $source->getSasToken(),
            implode(
                ' ',
                CopyCommandCsvOptionsHelper::getCsvCopyCommandOptions(
                    $importOptions,
                    $source->getCsvOptions(),
                ),
            ),
            implode(', ', $quotedFiles),
        );
    }
}
