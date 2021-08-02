<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Exasol\ToStage;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage\S3\SourceFile;
use Keboola\FileStorage\LineEnding\StringLineEndingDetectorHelper;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableReflection;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

class FromS3CopyIntoAdapter implements CopyAdapterInterface
{
    /** @var Connection */
    private $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * @param SourceFile $source
     * @param ExasolTableDefinition $destination
     * @param ExasolImportOptions $importOptions
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        TableDefinitionInterface $destination,
        ImportOptionsInterface $importOptions
    ): int {
        assert($source instanceof SourceFile);
        assert($destination instanceof ExasolTableDefinition);
        assert($importOptions instanceof ExasolImportOptions);

        $sql = $this->getCopyCommand($source, $destination, $importOptions);

        if ($sql !== null) {
            $this->connection->executeStatement($sql);
        }

        $ref = new ExasolTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );

        return $ref->getRowsCount();
    }

    private function getCopyCommand(
        Storage\S3\SourceFile $source,
        ExasolTableDefinition $destination,
        ExasolImportOptions $importOptions
    ): ?string {
        $destinationSchema = ExasolQuote::quoteSingleIdentifier($destination->getSchemaName());
        $destinationTable = ExasolQuote::quoteSingleIdentifier($destination->getTableName());

        return sprintf('
    IMPORT INTO %s.%s FROM CSV AT %s
USER %s IDENTIFIED BY %s
FILE %s',
            $destinationSchema,
            $destinationTable,
            ExasolQuote::quote($source->getBucketURL()),
            ExasolQuote::quote($source->getKey()),
            ExasolQuote::quote($source->getSecret()),
            ExasolQuote::quote($source->getFilePath())
        );
    }
}
