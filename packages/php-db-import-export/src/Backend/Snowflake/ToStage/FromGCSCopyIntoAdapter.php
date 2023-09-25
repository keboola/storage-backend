<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\ToStage;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeException;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\GCS\SourceFile;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Throwable;

class FromGCSCopyIntoAdapter implements CopyAdapterInterface
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
        ImportOptionsInterface $importOptions
    ): int {
        try {
            $files = $source->getManifestEntries();
            foreach (array_chunk($files, self::SLICED_FILES_CHUNK_SIZE) as $files) {
                $cmd = $this->getCopyCommand(
                    $source,
                    $destination,
                    $importOptions,
                    $files
                );
                $this->connection->executeStatement(
                    $cmd
                );
            }
        } catch (Throwable $e) {
            throw SnowflakeException::covertException($e);
        }

        $ref = new SnowflakeTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );

        return $ref->getRowsCount();
    }

    /**
     * @param string[] $files
     */
    private function getCopyCommand(
        Storage\GCS\SourceFile $source,
        SnowflakeTableDefinition $destination,
        SnowflakeImportOptions $importOptions,
        array $files
    ): string {
        $gcsPrefix = $source->getGcsPrefix();
        $csvOptions = $source->getCsvOptions();
        return sprintf(
            'COPY INTO %s.%s FROM %s
                STORAGE_INTEGRATION = %s, 
                FILE_FORMAT = (TYPE=CSV %s, NULL_IF=(\'\'))
                FILES = (%s)',
            SnowflakeQuote::quoteSingleIdentifier($destination->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($destination->getTableName()),
            SnowflakeQuote::quote($gcsPrefix),
            SnowflakeQuote::quoteSingleIdentifier($source->getStorageIntegrationName()),
            sprintf(
                '
        FIELD_DELIMITER = %s,
        SKIP_HEADER = %s,
        FIELD_OPTIONALLY_ENCLOSED_BY = %s,
        ESCAPE_UNENCLOSED_FIELD = %s
        ',
                SnowflakeQuote::quote($csvOptions->getDelimiter()),
                (string) $importOptions->getNumberOfIgnoredLines(),
                $csvOptions->getEnclosure() ? SnowflakeQuote::quote($csvOptions->getEnclosure()) : 'NONE',
                $csvOptions->getEscapedBy() ? SnowflakeQuote::quote($csvOptions->getEscapedBy()) : 'NONE',
            ),
            implode(
                ', ',
                array_map(
                    function ($file) use ($gcsPrefix) {
                        return SnowflakeQuote::quote(str_replace($gcsPrefix . '/', '', $file));
                    },
                    $files
                )
            )
        );
    }
}
