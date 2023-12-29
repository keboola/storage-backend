<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse\ToStage;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Synapse\Exception\Assert;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseException;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use LogicException;

final class ToStageImporter implements ToStageImporterInterface
{
    private const TIMER_TABLE_IMPORT = 'copyToStaging';

    private Connection $connection;

    private ?CopyAdapterInterface $adapter = null;

    public function __construct(
        Connection $connection,
        ?CopyAdapterInterface $adapter = null,
    ) {
        $this->connection = $connection;
        $this->adapter = $adapter;
    }

    public function importToStagingTable(
        Storage\SourceInterface $source,
        TableDefinitionInterface $destinationDefinition,
        ImportOptionsInterface $options,
    ): ImportState {
        assert($options instanceof SynapseImportOptions);
        assert($destinationDefinition instanceof SynapseTableDefinition);
        Assert::assertValidSource($source);
        Assert::assertColumnsOnTableDefinition($source, $destinationDefinition);
        $state = new ImportState($destinationDefinition->getTableName());

        $adapter = $this->getAdapter($source, $options);

        $state->startTimer(self::TIMER_TABLE_IMPORT);
        try {
            $state->addImportedRowsCount(
                $adapter->runCopyCommand(
                    $source,
                    $destinationDefinition,
                    $options,
                ),
            );
        } catch (Exception $e) {
            throw SynapseException::covertException($e);
        }
        $state->stopTimer(self::TIMER_TABLE_IMPORT);

        return $state;
    }

    private function getAdapter(
        Storage\SourceInterface $source,
        SynapseImportOptions $importOptions,
    ): CopyAdapterInterface {
        if ($this->adapter !== null) {
            return $this->adapter;
        }

        switch (true) {
            case $source instanceof Storage\ABS\SourceFile:
                $adapter = new FromABSCopyIntoAdapter($this->connection);
                break;
            case $source instanceof Storage\SqlSourceInterface:
                $adapter = $this->getAdapterForSqlSource($importOptions);
                break;
            default:
                throw new LogicException(
                    sprintf(
                        'No suitable adapter found for source: "%s".',
                        get_class($source),
                    ),
                );
        }
        return $adapter;
    }

    private function getAdapterForSqlSource(SynapseImportOptions $importOptions): CopyAdapterInterface
    {
        switch ($importOptions->getTableToTableAdapter()) {
            case SynapseImportOptions::TABLE_TO_TABLE_ADAPTER_CTAS:
                return new FromTableCTASAdapter($this->connection);
            case SynapseImportOptions::TABLE_TO_TABLE_ADAPTER_INSERT_INTO:
                return new FromTableInsertIntoAdapter($this->connection);
        }

        throw new LogicException(
            sprintf(
                'No suitable table to table adapter "%s".',
                $importOptions->getTableToTableAdapter(),
            ),
        );
    }
}
