<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\ToStage;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseException;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use LogicException;

final class ToStageImporter implements ToStageImporterInterface
{
    private const TIMER_TABLE_IMPORT = 'copyToStaging';

    private Connection $connection;

    public function __construct(
        Connection $connection
    ) {
        $this->connection = $connection;
    }

    public function importToStagingTable(
        Storage\SourceInterface $source,
        TableDefinitionInterface $destinationDefinition,
        ImportOptionsInterface $options
    ): ImportState {
        assert($destinationDefinition instanceof TeradataTableDefinition);
        assert($options instanceof TeradataImportOptions);
        $state = new ImportState($destinationDefinition->getTableName());

        $adapter = $this->getAdapter($source, $options, $state);

        $state->startTimer(self::TIMER_TABLE_IMPORT);
        try {
            $state->addImportedRowsCount(
                $adapter->runCopyCommand(
                    $source,
                    $destinationDefinition,
                    $options
                )
            );
        } catch (Exception $e) {
            throw SynapseException::covertException($e);
        }
        $state->stopTimer(self::TIMER_TABLE_IMPORT);

        return $state;
    }

    private function getAdapter(
        Storage\SourceInterface $source,
        TeradataImportOptions $options,
        ImportState $state
    ): CopyAdapterInterface {
        switch (true) {
            case $source instanceof Storage\S3\SourceFile:
                if ($options->getCsvImportAdapter() === TeradataImportOptions::CSV_ADAPTER_TPT) {
                    return new FromS3TPTAdapter($this->connection);
                }
                return new FromS3SPTAdapter($this->connection);
            case $source instanceof Storage\SqlSourceInterface:
                return new FromTableInsertIntoAdapter($this->connection);
            default:
                throw new LogicException(
                    sprintf(
                        'No suitable adapter found for source: "%s".',
                        get_class($source)
                    )
                );
        }
    }
}
