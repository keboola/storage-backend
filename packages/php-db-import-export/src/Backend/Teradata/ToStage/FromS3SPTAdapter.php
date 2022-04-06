<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\ToStage;

use Doctrine\DBAL\Connection;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\Teradata\Helper\BackendHelper;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\Db\ImportExport\Exception\Exception;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Symfony\Component\Process\Process;

/**
 * Stupid parallel transporter adapter
 */
class FromS3SPTAdapter implements CopyAdapterInterface
{
    private Connection $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * @param Storage\S3\SourceFile $source
     * @param TeradataTableDefinition $destination
     * @param ImportOptions $importOptions
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        TableDefinitionInterface $destination,
        ImportOptionsInterface $importOptions
    ): int {
        assert($source instanceof Storage\S3\SourceFile);
        assert($destination instanceof TeradataTableDefinition);
        assert($importOptions instanceof TeradataImportOptions);

        $csvOptions = $source->getCsvOptions();
        if ($csvOptions->getDelimiter() !== CsvOptions::DEFAULT_DELIMITER
            || $csvOptions->getEnclosure() !== CsvOptions::DEFAULT_ENCLOSURE
            || $csvOptions->getEscapedBy() !== CsvOptions::DEFAULT_ESCAPED_BY
        ) {
            throw new Exception('Non default csv options are not supported by STP.');
        }

        // empty manifest. SPT cannot import empty data
        if ($source->isSliced() && count($source->getManifestEntries()) === 0) {
            return 0;
        }

        foreach ($this->generateCmd($source, $destination, $importOptions) as $cmd) {
            $process = Process::fromShellCommandline($cmd);
            $process->setTimeout(60 * 60 * 24); // 24h
            $process->start();
            // check end of process
            $process->wait();

            // debug stuff
            //foreach ($process as $type => $data) {
            //    if ($process::OUT === $type) {
            //        echo "\nRead from stdout: " . $data;
            //    } else { // $process::ERR === $type
            //        echo "\nRead from stderr: " . $data;
            //    }
            //}
            if ($process->getExitCode() !== 0) {
                $qb = new TeradataTableQueryBuilder();
                // drop destination table it's not usable
                $this->connection->executeStatement($qb->getDropTableCommand(
                    $destination->getSchemaName(),
                    $destination->getTableName()
                ));

                throw new Exception('Import failed: ' . implode(' , ', iterator_to_array($process)));
            }
        }

        $ref = new TeradataTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );

        return $ref->getRowsCount();
    }

    /**
     * @return \Generator<string>
     */
    private function generateCmd(
        Storage\S3\SourceFile $source,
        TeradataTableDefinition $destination,
        TeradataImportOptions $importOptions
    ): \Generator {
        $filesToImport = $source->getManifestEntries();
        $endsWithGz = substr_compare(
            $filesToImport[0],
            '.gz',
            strlen($filesToImport[0]) - strlen('.gz'),
            strlen('.gz')
        );
        $cmd = [
            'spt', // cmd
            '-a',
            BackendHelper::quoteValue($source->getKey()), // AWS KEY
            '-s',
            BackendHelper::quoteValue($source->getSecret()),  // AWS SECRET
            '-b',
            BackendHelper::quoteValue($source->getBucket()), // Bucket
            '-r',
            BackendHelper::quoteValue($source->getRegion()), // s3 region
            '-th',
            BackendHelper::quoteValue($importOptions->getTeradataHost()), // Teradata host
            '-tp',
            $importOptions->getTeradataPort(), // Teradata port
            '-tu',
            BackendHelper::quoteValue($importOptions->getTeradataUser()), // Teradata user
            '-tps',
            BackendHelper::quoteValue($importOptions->getTeradataPassword()), // Teradata password
            '-td',
            'dbc', // connection database
            '-tt', // tableName
            TeradataQuote::quoteSingleIdentifier($destination->getTableName()),
            '-ts', // database
            TeradataQuote::quoteSingleIdentifier($destination->getSchemaName()),
            '-il',
            $importOptions->getNumberOfIgnoredLines(),
        ];
        // is compressed ?
        if ($endsWithGz === 0) {
            $cmd[] = '-c';
        }

        $s3Prefix = $source->getS3Prefix() . '/';
        foreach (array_chunk($filesToImport, ToStageImporterInterface::SLICED_FILES_CHUNK_SIZE) as $entries) {
            $quotedFiles = array_map(
                static function ($entry) use ($s3Prefix) {
                    return BackendHelper::quoteValue(strtr($entry, [$s3Prefix => '']));
                },
                $entries
            );

            yield implode(' ', array_merge($cmd, $quotedFiles));
        }
    }
}
