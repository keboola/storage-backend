<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\ToStage;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\Teradata\Helper\BackendHelper;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\Exception\FailedTPTLoadException;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\FileStorage\Path\RelativePath;
use Keboola\FileStorage\S3\S3Provider;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Keboola\Temp\Temp;
use Symfony\Component\Process\Process;

/**
 * @todo: get logs out on success
 */
class FromS3TPTAdapter implements CopyAdapterInterface
{
    private const TPT_TIMEOUT = 60 * 60 * 6; // 6 hours

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

        // empty manifest. TPT cannot import empty data
        if ($source->isSliced() && count($source->getManifestEntries()) === 0) {
            return 0;
        }

        /**
         * @var Temp $temp
         */
        [
            $temp,
            $logTable,
            $errTable,
            $errTable2,
            $processCmd,
        ] = $this->generateTPTScript($source, $destination, $importOptions);

        $process = new Process(
            $processCmd,
            null,
            [
                'AWS_ACCESS_KEY_ID' => $source->getKey(),
                'AWS_SECRET_ACCESS_KEY' => $source->getSecret(),
            ]
        );
        $process->setTimeout(self::TPT_TIMEOUT);
        $process->start();
        // check end of process
        $process->wait();

        // debug stuff
//        foreach ($process as $type => $data) {
//            if ($process::OUT === $type) {
//                echo "\nRead from stdout: " . $data;
//            } else { // $process::ERR === $type
//                echo "\nRead from stderr: " . $data;
//            }
//        }
        $qb = new SqlBuilder();
        $isTableExists = function (string $databaseName, string $tableName) use ($qb) {
            return (bool) $this->connection->fetchOne($qb->getTableExistsCommand($databaseName, $tableName));
        };

        $logContent = null;
        if ($isTableExists($destination->getSchemaName(), $logTable)) {
            $logContent = $this->connection->fetchAllAssociative(
                sprintf(
                    'SELECT * FROM %s.%s',
                    TeradataQuote::quoteSingleIdentifier($destination->getSchemaName()),
                    TeradataQuote::quoteSingleIdentifier($logTable)
                )
            );
            $this->connection->executeStatement($qb->getDropTableUnsafe($destination->getSchemaName(), $logTable));
        }
        $errContent = null;
        if ($isTableExists($destination->getSchemaName(), $errTable)) {
            $errContent = $this->connection->fetchAllAssociative(sprintf(
                'SELECT * FROM %s.%s',
                TeradataQuote::quoteSingleIdentifier($destination->getSchemaName()),
                TeradataQuote::quoteSingleIdentifier($errTable)
            ));
            $this->connection->executeStatement($qb->getDropTableUnsafe($destination->getSchemaName(), $errTable));
        }
        $err2Content = null;
        if ($isTableExists($destination->getSchemaName(), $errTable2)) {
            $err2Content = $this->connection->fetchAllAssociative(sprintf(
                'SELECT * FROM %s.%s',
                TeradataQuote::quoteSingleIdentifier($destination->getSchemaName()),
                TeradataQuote::quoteSingleIdentifier($errTable2)
            ));
            $this->connection->executeStatement($qb->getDropTableUnsafe($destination->getSchemaName(), $errTable2));
        }
        // TODO find the way how to get this out

        if ($process->getExitCode() !== 0 || $errContent || $err2Content) {
            $qb = new TeradataTableQueryBuilder();
            // drop destination table it's not usable
            $this->connection->executeStatement($qb->getDropTableCommand(
                $destination->getSchemaName(),
                $destination->getTableName()
            ));

            throw new FailedTPTLoadException(
                $process->getErrorOutput(),
                $process->getOutput(),
                $process->getExitCode(),
                $this->getLogData($temp),
                $logContent,
                $errContent,
                $err2Content
            );
        }

        $ref = new TeradataTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );

        return $ref->getRowsCount();
    }

    private function getLogData(Temp $temp): string
    {
        if (file_exists($temp->getTmpFolder() . '/import-1.out')) {
            return file_get_contents($temp->getTmpFolder() . '/import-1.out') ?: 'unable to get error';
        }

        return 'unable to get error';
    }

    /**
     * @return array{0: Temp, 1:string, 2:string, 3:string, 4: string[]}
     */
    private function generateTPTScript(
        Storage\S3\SourceFile $source,
        TeradataTableDefinition $destination,
        TeradataImportOptions $importOptions
    ): array {
        $temp = new Temp();
        $temp->initRunFolder();
        $folder = $temp->getTmpFolder();
        $target = sprintf(
            '%s.%s',
            TeradataQuote::quoteSingleIdentifier($destination->getSchemaName()),
            TeradataQuote::quoteSingleIdentifier($destination->getTableName()),
        );

        if ($source->isSliced()) {
            $mask = BackendHelper::getMask($source);
            $path = RelativePath::createFromRootAndPath(new S3Provider(), $source->getBucket(), $mask);
            $moduleStr = sprintf(
                // phpcs:ignore
                'AccessModuleInitStr = \'S3Region="%s" S3Bucket="%s" S3Prefix="%s" S3Object="%s" S3SinglePartFile=True\'',
                $source->getRegion(),
                $path->getRoot(),
                $path->getPathWithoutRoot() . '/',
                $path->getFileName()
            );
        } else {
            $moduleStr = sprintf(
                // phpcs:ignore
                'AccessModuleInitStr = \'S3Region="%s" S3Bucket="%s" S3Prefix="%s" S3Object="%s" S3SinglePartFile=True\'',
                $source->getRegion(),
                $source->getBucket(),
                $source->getPrefix(),
                $source->getFileName()
            );
        }
        $tptScript = <<<EOD
USING CHARACTER SET UTF8
DEFINE JOB IMPORT_TO_TERADATA
DESCRIPTION 'Import data to Teradata from Amazon S3'
(

    SET TargetTable = '$target';

    STEP IMPORT_THE_DATA
    (
        APPLY \$INSERT TO OPERATOR (\$LOAD)
        SELECT * FROM OPERATOR (\$FILE_READER ()
            ATTR
            (
                AccessModuleName = 'libs3axsmod.so',
                $moduleStr
            )
        );

    );
);
EOD;

        file_put_contents($folder . '/import_script.tpt', $tptScript);

        $host = $importOptions->getTeradataHost();
        $user = $importOptions->getTeradataUser();
        $pass = $importOptions->getTeradataPassword();
        $csvOptions = $source->getCsvOptions();
        $delimiter = $csvOptions->getDelimiter();
        if ($delimiter === "\t") {
            $delimiter = 'TAB';
        }
        $enclosure = $csvOptions->getEnclosure();
        if ($enclosure === '\'') {
            $enclosure = '\\\'';
        }
        $escapedBy = $csvOptions->getEscapedBy();
        if ($escapedBy !== '') {
            $escapedBy = sprintf(',FileReaderEscapeQuoteDelimiter = \'%s\'', $escapedBy);
        }
        $ignoredLines = $importOptions->getNumberOfIgnoredLines();

        $quotedDestination = TeradataQuote::quoteSingleIdentifier($destination->getSchemaName());
        $tablesPrefix = BackendHelper::generateTempTableName();
        $logTable = $tablesPrefix . '_log';
        $logTableQuoted = $quotedDestination . '.' . TeradataQuote::quoteSingleIdentifier($logTable);
        $errTable1 = $tablesPrefix . '_e1';
        $errTableQuoted = $quotedDestination . '.' . TeradataQuote::quoteSingleIdentifier($errTable1);
        $errTable2 = $tablesPrefix . '_e2';
        $errTable2Quoted = $quotedDestination . '.' . TeradataQuote::quoteSingleIdentifier($errTable2);

        $jobVariableFile = <<<EOD
/********************************************************/
/* TPT attributes - Common for all Samples              */
/********************************************************/
TargetTdpId = '$host'
,TargetUserName = '$user'
,TargetUserPassword = '$pass'
,TargetErrorList = [ '3706','3803','3807' ]
,DDLPrivateLogName = 'DDL_OPERATOR_LOG'

/********************************************************/
/* TPT LOAD Operator attributes                         */
/********************************************************/
,LoadPrivateLogName = 'LOAD_OPERATOR_LOG'
,LoadTargetTable = '${target}'
,LoadLogTable = '${logTableQuoted}'
,LoadErrorTable1 = '${errTableQuoted}'
,LoadErrorTable2 = '${errTable2Quoted}'

/********************************************************/
/* TPT DataConnector Producer Operator                  */
/********************************************************/
,FileReaderFormat = 'Delimited'
,FileReaderOpenMode = 'Read'
,FileReaderTextDelimiter = '$delimiter'
,FileReaderSkipRows = $ignoredLines
,FileReaderOpenQuoteMark = '$enclosure'
,FileReaderQuotedData = 'Optional'
$escapedBy
,FileReaderTruncateColumns = 'Yes'
EOD;

        file_put_contents($folder . '/import_vars.txt', $jobVariableFile);
        file_put_contents('./import_vars.txt', $jobVariableFile);

        return [
            $temp,
            $logTable,
            $errTable1,
            $errTable2,
            [
                'tbuild',
                '-j',
                'import',
                '-L',
                $folder,
                '-f',
                $folder . '/import_script.tpt',
                '-v',
                $folder . '/import_vars.txt',
            ],
        ];
    }
}
