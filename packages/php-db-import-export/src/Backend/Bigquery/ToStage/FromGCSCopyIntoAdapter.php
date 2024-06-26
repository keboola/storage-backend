<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\ToStage;

use Exception;
use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryException;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Connection\Bigquery\BigQueryClientWrapper;
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
        ImportOptionsInterface $importOptions,
    ): int {
        assert($source instanceof Storage\GCS\SourceFile);
        assert($destination instanceof BigqueryTableDefinition);
        assert($importOptions instanceof ImportOptions);

        $entries = $source->getManifestEntries(Storage\GCS\SourceFile::PROTOCOL_GS);

        if (count($entries) === 0) {
            return 0;
        }

        $schema = $this->bqClient->dataset($destination->getSchemaName());

        if (!$schema->exists()) {
            throw new Exception(sprintf('Schema "%s" does not exist', $destination->getSchemaName()));
        }

        $table = $schema->table($destination->getTableName());

        if (!$table->exists()) {
            throw new Exception(sprintf('Table "%s" does not exist', $destination->getTableName()));
        }

        // fix ASCII0 error
        $options = [
            'configuration' => [
                'load' => [
                    'preserveAsciiControlCharacters' => true,
                ],
            ],
        ];
        // parameter is no nullable, but when I set first entry and removed it from array file won't import
        // so this only possible solution
        $loadConfig = $table->loadFromStorage(reset($entries), $options)
            ->sourceFormat('CSV')
            ->sourceUris($entries)
            ->autodetect(false)
            ->fieldDelimiter($source->getCsvOptions()->getDelimiter())
            ->skipLeadingRows($importOptions->getNumberOfIgnoredLines())
            ->quote($source->getCsvOptions()->getEnclosure())
            ->allowQuotedNewlines(true);

        if ($importOptions->importAsNull() !== []) {
            // BigQuery allows only one null marker
            // we implicitly use the first one and ignore others if any
            $nullMarker = $importOptions->importAsNull()[0];
        } else {
            $nullMarker = $this->getDefaultAsNullString();
        }
        $loadConfig->nullMarker($nullMarker);

        $job = $this->bqClient->runJob($loadConfig);
        // check if the job has errors
        if (isset($job->info()['status']['errorResult'])) {
            throw BigqueryException::createExceptionFromJobResult($job->info());
        }

        $ref = new BigqueryTableReflection(
            $this->bqClient,
            $destination->getSchemaName(),
            $destination->getTableName(),
        );

        return $ref->getRowsCount();
    }

    private function getDefaultAsNullString(): string
    {
        return '__KEBOOLA_IMPORT_AS_NULL__' . uniqid('', true);
    }
}
