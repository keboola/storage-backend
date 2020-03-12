<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;

class SnowflakeExportAdapter implements BackendExportAdapterInterface
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

        $compression = $exportOptions->isCompresed() ? "COMPRESSION='GZIP'" : "COMPRESSION='NONE'";

        $from = $source->getFromStatement();

        $sql = sprintf(
            'COPY INTO \'%s%s\' 
FROM %s
CREDENTIALS=(AZURE_SAS_TOKEN=\'%s\')
FILE_FORMAT = (
    TYPE = \'CSV\'
    FIELD_DELIMITER = \',\'
    FIELD_OPTIONALLY_ENCLOSED_BY = \'\"\'
    %s
    TIMESTAMP_FORMAT = \'YYYY-MM-DD HH24:MI:SS\'
)
MAX_FILE_SIZE=50000000',
            $this->destination->getContainerUrl(BaseFile::PROTOCOL_AZURE),
            $this->destination->getFilePath(),
            $from,
            $this->destination->getSasToken(),
            $compression
        );

        $connection->fetchAll($sql, $source->getQueryBindings());
    }
}
