<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\SQLServerPlatform;
use Keboola\Db\ImportExport\Backend\Synapse\SqlCommandBuilder;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportAdapterInterface;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\FileStorage\LineEnding\StringLineEndingDetectorHelper;

class SynapseImportAdapter implements SynapseImportAdapterInterface
{
    /** @var Connection */
    private $connection;

    /** @var \Doctrine\DBAL\Platforms\AbstractPlatform|SQLServerPlatform */
    private $platform;

    /** @var SqlCommandBuilder */
    private $sqlBuilder;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        $this->platform = $connection->getDatabasePlatform();
        $this->sqlBuilder = new SqlCommandBuilder($this->connection);
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
     * @param SynapseImportOptions $importOptions
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptionsInterface $importOptions,
        string $stagingTableName
    ): int {
        $sql = $this->getCopyCommand($source, $destination, $importOptions, $stagingTableName);

        if ($sql !== null) {
            $this->connection->exec($sql);
        }

        $rows = $this->connection->fetchAll($this->sqlBuilder->getTableItemsCountCommand(
            $destination->getSchema(),
            $stagingTableName
        ));

        return (int) $rows[0]['count'];
    }

    private function getCopyCommand(
        Storage\ABS\SourceFile $source,
        Storage\Synapse\Table $destination,
        SynapseImportOptions $importOptions,
        string $stagingTableName
    ): ?string {

        $destinationSchema = $this->platform->quoteSingleIdentifier($destination->getSchema());
        $destinationTable = $this->platform->quoteSingleIdentifier($stagingTableName);

        switch ($importOptions->getImportCredentialsType()) {
            case SynapseImportOptions::CREDENTIALS_SAS:
                $sasToken = $source->getSasToken();
                $credentials = sprintf('IDENTITY=\'Shared Access Signature\', SECRET=\'?%s\'', $sasToken);
                break;
            case SynapseImportOptions::CREDENTIALS_MANAGED_IDENTITY:
                $credentials = 'IDENTITY=\'Managed Identity\'';
                break;
            default:
                throw new \InvalidArgumentException(sprintf(
                    'Unknown Synapse import credentials type "%s".',
                    $importOptions->getImportCredentialsType()
                ));
        }

        $rowTerminator = '';
        if ($source->getLineEnding() === StringLineEndingDetectorHelper::EOL_UNIX) {
            $rowTerminator = 'ROWTERMINATOR=\'0x0A\',';
        }

        $fieldDelimiter = $this->connection->quote($source->getCsvOptions()->getDelimiter());
        $firstRow = '';
        if ($importOptions->getNumberOfIgnoredLines() !== 0) {
            $firstRow = sprintf(',FIRSTROW=%s', $importOptions->getNumberOfIgnoredLines() + 1);
        }
        $enclosure = $this->connection->quote($source->getCsvOptions()->getEnclosure());

        $entries = $source->getManifestEntries(SourceFile::PROTOCOL_HTTPS);

        if (count($entries) === 0) {
            return null;
        }

        $entries = array_map(function ($entry) {
            return $this->connection->quote($entry);
        }, $entries);

        $entries = implode(', ', $entries);

        return <<< EOT
COPY INTO $destinationSchema.$destinationTable
FROM $entries
WITH (
    FILE_TYPE='CSV',
    CREDENTIAL=($credentials),
    FIELDQUOTE=$enclosure,
    FIELDTERMINATOR=$fieldDelimiter,
    ENCODING = 'UTF8',
    $rowTerminator
    IDENTITY_INSERT = 'OFF'
    $firstRow
)
EOT
            ;
    }
}
