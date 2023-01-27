<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\ToStage;

use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\Db\ImportExport\Backend\Bigquery\Helper\CopyCommandCsvOptionsHelper;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

class FromGCSCopyIntoAdapter implements CopyAdapterInterface
{
    private BigQueryClient $bqClient;

    public function __construct(BigQueryClient $bqClient)
    {
        $this->bqClient = $bqClient;
    }

    public function runCopyCommand(
        Storage\SourceInterface $source,
        TableDefinitionInterface $destination,
        ImportOptionsInterface $importOptions
    ): int {
        assert($source instanceof Storage\GCS\SourceFile);
        assert($destination instanceof BigqueryTableDefinition);
        assert($importOptions instanceof ImportOptions);

        $destinationSchema = BigqueryQuote::quoteSingleIdentifier($destination->getSchemaName());
        $destinationTable = BigqueryQuote::quoteSingleIdentifier($destination->getTableName());

        $entries = $source->getManifestEntries(Storage\GCS\SourceFile::PROTOCOL_GS);

        if (count($entries) === 0) {
            return 0;
        }

        $entries = array_map(function ($entry) {
            return BigqueryQuote::quote($entry);
        }, $entries);

        $entries = implode(', ', $entries);

        $options = CopyCommandCsvOptionsHelper::getCsvCopyCommandOptions($importOptions, $source->getCsvOptions());

        $sql = sprintf(
            'LOAD DATA INTO %s.%s FROM FILES (%s, format = \'CSV\', uris = [%s]);',
            $destinationSchema,
            $destinationTable,
            implode(',', $options),
            $entries
        );

        $query = $this->bqClient->query($sql);
        $this->bqClient->runQuery($query);

        $ref = new BigqueryTableReflection(
            $this->bqClient,
            $destination->getSchemaName(),
            $destination->getTableName()
        );

        return $ref->getRowsCount();
    }
}
