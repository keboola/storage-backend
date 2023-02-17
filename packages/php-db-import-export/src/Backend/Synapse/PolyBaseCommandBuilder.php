<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Generator;
use InvalidArgumentException;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Escaping\SynapseQuote;

class PolyBaseCommandBuilder
{

    public function getCredentialsQuery(
        string $credentialsId,
        string $exportCredentialsType,
        ?string $blobMasterKey = null
    ): string {
        $credentialsId = SynapseQuote::quoteSingleIdentifier($credentialsId);

        switch ($exportCredentialsType) {
            case SynapseExportOptions::CREDENTIALS_MANAGED_IDENTITY:
                return <<< EOT
CREATE DATABASE SCOPED CREDENTIAL $credentialsId
WITH
    IDENTITY = 'Managed Service Identity'
;
EOT;
            case SynapseExportOptions::CREDENTIALS_MASTER_KEY:
                assert($blobMasterKey !== null);
                $blobMasterKey = SynapseQuote::quote($blobMasterKey);
                return <<< EOT
CREATE DATABASE SCOPED CREDENTIAL $credentialsId
WITH
    IDENTITY = 'user',
    SECRET = $blobMasterKey
;
EOT;
            default:
                throw new InvalidArgumentException(sprintf(
                    'Unknown Synapse export credentials type "%s".',
                    $exportCredentialsType
                ));
        }
    }

    public function getDataSourceQuery(
        string $dataSourceId,
        string $containerUrl,
        string $credentialsId
    ): string {
        $dataSourceId = SynapseQuote::quoteSingleIdentifier($dataSourceId);
        $containerUrl = SynapseQuote::quote($containerUrl);
        $credentialsId = SynapseQuote::quoteSingleIdentifier($credentialsId);

        return <<<EOT
CREATE EXTERNAL DATA SOURCE $dataSourceId
WITH 
(
    TYPE = HADOOP,
    LOCATION = $containerUrl,
    CREDENTIAL = $credentialsId
);
EOT;
    }

    public function getFileFormatQuery(string $fileFormatId, string $dateFormat, string $compression): string
    {
        $fileFormatId = SynapseQuote::quoteSingleIdentifier($fileFormatId);

        return <<<EOT
CREATE EXTERNAL FILE FORMAT $fileFormatId
WITH
(
    FORMAT_TYPE = DelimitedText,
    FORMAT_OPTIONS 
    (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        DATE_FORMAT = '$dateFormat',
        USE_TYPE_DEFAULT = FALSE
    )
    $compression
);
EOT;
    }

    public function getPolyBaseCleanUpQueries(
        string $fileFormatId,
        string $dataSourceId,
        string $credentialsId,
        ?string $tableId
    ): Generator {
        if ($tableId !== null) {
            $tableId = SynapseQuote::quoteSingleIdentifier($tableId);
            yield sprintf('DROP EXTERNAL TABLE %s', $tableId);
        }
        $fileFormatId = SynapseQuote::quoteSingleIdentifier($fileFormatId);
        yield sprintf('DROP EXTERNAL FILE FORMAT %s', $fileFormatId);
        $dataSourceId = SynapseQuote::quoteSingleIdentifier($dataSourceId);
        yield sprintf('DROP EXTERNAL DATA SOURCE %s', $dataSourceId);
        $credentialsId = SynapseQuote::quoteSingleIdentifier($credentialsId);
        yield sprintf('DROP DATABASE SCOPED CREDENTIAL %s', $credentialsId);
    }

    public function getExternalTableQuery(
        Storage\SqlSourceInterface $source,
        string $tableId,
        string $exportPath,
        string $dataSourceId,
        string $fileFormatId
    ): string {
        $tableId = SynapseQuote::quoteSingleIdentifier($tableId);
        $dataSourceId = SynapseQuote::quoteSingleIdentifier($dataSourceId);
        $fileFormatId = SynapseQuote::quoteSingleIdentifier($fileFormatId);
        $from = $source->getFromStatement();

        return <<<EOT
CREATE EXTERNAL TABLE $tableId
WITH 
(
    LOCATION='$exportPath',
    DATA_SOURCE = $dataSourceId,
    FILE_FORMAT = $fileFormatId
)
AS
$from
EOT;
    }
}
