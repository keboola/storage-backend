<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\Db\ImportExport\Storage\Synapse\Table;
use Keboola\Db\ImportExport\Storage\SourceInterface;

class SynapseImportAdapter implements BackendImportAdapterInterface
{
    /**
     * @var SourceFile
     */
    private $source;

    /**
     * @param SourceFile $source
     */
    public function __construct(SourceInterface $source)
    {
        $this->source = $source;
    }

    /**
     * phpcs:disable SlevomatCodingStandard.TypeHints.TypeHintDeclaration.MissingParameterTypeHint
     * @param Table $destination
     * @param Connection $connection
     */
    public function getCopyCommands(
        DestinationInterface $destination,
        ImportOptions $importOptions,
        string $stagingTableName,
        $connection = null
    ): array {

        if ($connection === null || !$connection instanceof Connection) {
            throw new \Exception(sprintf('Connection must be instance of "%s"', Connection::class));
        }

        $platform = $connection->getDatabasePlatform();
        $sasToken = $this->source->getSasToken();
        $destinationSchema = $platform->quoteSingleIdentifier($destination->getSchema());
        $destinationTable = $platform->quoteSingleIdentifier($stagingTableName);

        $fieldDelimiter = $connection->quote($this->source->getCsvOptions()->getDelimiter());
        $firstRow = '';
        if ($importOptions->getNumberOfIgnoredLines() !== 0) {
            $firstRow = sprintf(',FIRSTROW=%s', $importOptions->getNumberOfIgnoredLines() + 1);
        }
        $enclosure = $connection->quote($this->source->getCsvOptions()->getEnclosure());

        $entries = $this->source->getManifestEntries(SourceFile::PROTOCOL_HTTPS);

        if (count($entries) === 0) {
            return [];
        }

        foreach ($entries as &$entry) {
            $entry = $connection->quote($entry);
        }
        unset($entry);

        $entries = implode(', ', $entries);

        return [
            <<< EOT
COPY INTO $destinationSchema.$destinationTable
FROM $entries
WITH (
    FILE_TYPE='CSV',
    CREDENTIAL=(IDENTITY='Shared Access Signature', SECRET='?$sasToken'),
    FIELDQUOTE=$enclosure,
    FIELDTERMINATOR=$fieldDelimiter,
    ENCODING = 'UTF8',
    ROWTERMINATOR='0x0A',
    IDENTITY_INSERT = 'OFF'
    $firstRow
)
EOT
            ,
        ];
    }
}
