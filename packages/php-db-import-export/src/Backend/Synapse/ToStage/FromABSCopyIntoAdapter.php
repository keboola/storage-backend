<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse\ToStage;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage\ABS\SourceFile;
use Keboola\FileStorage\LineEnding\StringLineEndingDetectorHelper;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Escaping\SynapseQuote;
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

class FromABSCopyIntoAdapter implements CopyAdapterInterface
{
    /** @var Connection */
    private $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * @param SourceFile $source
     * @param SynapseTableDefinition $destination
     * @param SynapseImportOptions $importOptions
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        TableDefinitionInterface $destination,
        ImportOptionsInterface $importOptions
    ): int {
        assert($source instanceof SourceFile);
        assert($destination instanceof SynapseTableDefinition);
        assert($importOptions instanceof SynapseImportOptions);

        $sql = $this->getCopyCommand($source, $destination, $importOptions);

        if ($sql !== null) {
            $this->connection->executeStatement($sql);
        }

        $ref = new SynapseTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );

        return $ref->getRowsCount();
    }

    private function getCopyCommand(
        Storage\ABS\SourceFile $source,
        SynapseTableDefinition $destination,
        SynapseImportOptions $importOptions
    ): ?string {
        $destinationSchema = SynapseQuote::quoteSingleIdentifier($destination->getSchemaName());
        $destinationTable = SynapseQuote::quoteSingleIdentifier($destination->getTableName());

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

        $fieldDelimiter = SynapseQuote::quote($source->getCsvOptions()->getDelimiter());
        $firstRow = '';
        if ($importOptions->getNumberOfIgnoredLines() !== 0) {
            $firstRow = sprintf(',FIRSTROW=%s', $importOptions->getNumberOfIgnoredLines() + 1);
        }
        $enclosure = SynapseQuote::quote($source->getCsvOptions()->getEnclosure());

        $entries = $source->getManifestEntries(SourceFile::PROTOCOL_HTTPS);

        if (count($entries) === 0) {
            return null;
        }

        $entries = array_map(function ($entry) {
            return SynapseQuote::quote($entry);
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
