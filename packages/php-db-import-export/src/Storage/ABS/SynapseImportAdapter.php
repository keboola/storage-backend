<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\SQLServerPlatform;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportAdapterInterface;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;

class SynapseImportAdapter implements SynapseImportAdapterInterface
{
    /** @var Connection */
    private $connection;

    /** @var \Doctrine\DBAL\Platforms\AbstractPlatform|SQLServerPlatform */
    private $platform;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        $this->platform = $connection->getDatabasePlatform();
    }

    public static function isSupported(Storage\SourceInterface $source, Storage\DestinationInterface $destination): bool
    {
        if (!$source instanceof Storage\ABS\SourceFile) {
            return false;
        }
        if (!$destination instanceof Storage\Synapse\Table) {
            return false;
        }
        return true;
    }

    /**
     * @param Storage\ABS\SourceFile $source
     * @param Storage\Synapse\Table $destination
     */
    public function getCopyCommands(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptions $importOptions,
        string $stagingTableName
    ): array {
        $sasToken = $source->getSasToken();
        $destinationSchema = $this->platform->quoteSingleIdentifier($destination->getSchema());
        $destinationTable = $this->platform->quoteSingleIdentifier($stagingTableName);

        $fieldDelimiter = $this->connection->quote($source->getCsvOptions()->getDelimiter());
        $firstRow = '';
        if ($importOptions->getNumberOfIgnoredLines() !== 0) {
            $firstRow = sprintf(',FIRSTROW=%s', $importOptions->getNumberOfIgnoredLines() + 1);
        }
        $enclosure = $this->connection->quote($source->getCsvOptions()->getEnclosure());

        $entries = $source->getManifestEntries(SourceFile::PROTOCOL_HTTPS);

        if (count($entries) === 0) {
            return [];
        }

        foreach ($entries as &$entry) {
            $entry = $this->connection->quote($entry);
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
