<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeExportAdapterInterface;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;

class SnowflakeExportAdapter implements SnowflakeExportAdapterInterface
{
    /** @var Connection */
    private $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
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
        $sql = sprintf(
            'COPY INTO \'%s%s\' 
FROM (%s)
CREDENTIALS=(AZURE_SAS_TOKEN=\'%s\')
FILE_FORMAT = (
    TYPE = \'CSV\'
    FIELD_DELIMITER = \',\'
    FIELD_OPTIONALLY_ENCLOSED_BY = \'\"\'
    %s
    TIMESTAMP_FORMAT = \'YYYY-MM-DD HH24:MI:SS\'
)
MAX_FILE_SIZE=50000000',
            $destination->getContainerUrl(BaseFile::PROTOCOL_AZURE),
            $destination->getFilePath(),
            $source->getFromStatement(),
            $destination->getSasToken(),
            $exportOptions->isCompressed() ? "COMPRESSION='GZIP'" : "COMPRESSION='NONE'"
        );

        $this->connection->fetchAll($sql, $source->getQueryBindings());
    }
}
