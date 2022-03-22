<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\ToStage;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseException;
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
        $state = new ImportState($destinationDefinition->getTableName());

        $adapter = $this->getAdapter($source);

        $state->startTimer(self::TIMER_TABLE_IMPORT);
        try {
            $state->addImportedRowsCount(
                $adapter->runCopyCommand(
                    $source,
                    $destinationDefinition,
                    $options
                )
            );
        } catch (\Doctrine\DBAL\Exception $e) {
            throw SynapseException::covertException($e);
        }
        $state->stopTimer(self::TIMER_TABLE_IMPORT);

        return $state;
    }

    private function getAdapter(Storage\SourceInterface $source): CopyAdapterInterface
    {
        switch (true) {
            case $source instanceof Storage\S3\SourceFile:
                $adapter = new FromS3TPTAdapter($this->connection);
                break;
            case $source instanceof Storage\SqlSourceInterface:
                $adapter = new FromTableInsertIntoAdapter($this->connection);
                break;
            default:
                throw new LogicException(
                    sprintf(
                        'No suitable adapter found for source: "%s".',
                        get_class($source)
                    )
                );
        }
        return $adapter;
    }
}
