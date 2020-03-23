<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

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
     * @param Storage\SqlSourceInterface $source
     * @throws \Exception
     */
    public function getCopyCommand(
        Storage\SourceInterface $source,
        ExportOptions $exportOptions
    ): string {
        $compression = $exportOptions->isCompresed() ? "COMPRESSION='GZIP'" : "COMPRESSION='NONE'";

        if (!$source instanceof Storage\SqlSourceInterface) {
            throw new \Exception(sprintf(
                'Source "%s" must implement "%s".',
                get_class($source),
                Storage\SqlSourceInterface::class
            ));
        }

        $from = $source->getFromStatement();

//TODO: encryption "ENCRYPTION = (TYPE = 'AZURE_CSE' master_key = '%s')"
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

        return $sql;
    }
}
