<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\ToStage;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\Teradata\Helper\BackendHelper;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\Exception\FailedTPTLoadException;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Keboola\Temp\Temp;
use Symfony\Component\Process\Process;

/**
 * @todo: get logs out on success
 * @todo: fix log tables exists statement
 * @todo: test sliced files
 */
class FromS3TPTAdapter implements CopyAdapterInterface
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
        $process->start();

        //// debug stuff
        //foreach ($process as $type => $data) {
        //    if ($process::OUT === $type) {
        //        echo "\nRead from stdout: " . $data;
        //    } else { // $process::ERR === $type
        //        echo "\nRead from stderr: " . $data;
        //    }
        //}
        $isTableExists = function (string $tableName, string $databaseName) {
            return (bool) $this->connection->fetchOne(sprintf('SELECT 1 FROM dbc.TablesV WHERE TableName = %s AND DataBaseName = %s', TeradataQuote::quote($tableName), TeradataQuote::quote($databaseName)));
        };

        $logContent = null;
        if ($isTableExists($logTable, $destination->getSchemaName())) {
            $logContent = $this->connection->fetchAllAssociative('SELECT * FROM ' . TeradataQuote::quoteSingleIdentifier($logTable));
            $this->connection->executeStatement('DROP TABLE ' . $logTable);
        }
        $errContent = null;
        if ($isTableExists($errTable, $destination->getSchemaName())) {
            $errContent = $this->connection->fetchAllAssociative('SELECT * FROM ' . TeradataQuote::quoteSingleIdentifier($errTable));
            $this->connection->executeStatement('DROP TABLE ' . $errTable);
        }
        $err2Content = null;
        if ($isTableExists($errTable2, $destination->getSchemaName())) {
            $err2Content = $this->connection->fetchAllAssociative('SELECT * FROM ' . TeradataQuote::quoteSingleIdentifier($errTable2));
            $this->connection->executeStatement('DROP TABLE ' . $errTable2);
        }
        // find the way how to get this out

        if ($process->getExitCode() !== 0) {
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
                file_get_contents($temp->getTmpFolder() . '/import-1.out'),
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
            "%s.%s",
            TeradataQuote::quoteSingleIdentifier($destination->getSchemaName()),
            TeradataQuote::quoteSingleIdentifier($destination->getTableName()),
        );

        if ($source->isSliced()) {
            $moduleStr = sprintf(
                'AccessModuleInitStr = \'S3Region=%s S3Bucket=%s S3Prefix="%s/" S3SinglePartFile=False\'',
                $source->getRegion(),
                $source->getBucket(),
                $source->getPrefix()
            );
        } else {
            $moduleStr = sprintf(
                'AccessModuleInitStr = \'S3Region=%s S3Bucket=%s S3Prefix="%s/" S3Object=%s S3SinglePartFile=True\'',
                $source->getRegion(),
                $source->getBucket(),
                $source->getPrefix(),
                $source->getFileName()
            );
        }
        $tptScript = <<<EOD
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
        $enclosure = $csvOptions->getEnclosure();
        if ($enclosure === '\'') {
            $enclosure = '\\\'';
        }
        $escapedBy = $csvOptions->getEscapedBy();
        if ($escapedBy !== '') {
            $escapedBy = sprintf(',FileReaderEscapeQuoteDelimiter = \'%s\'', $escapedBy);
        }
        $ignoredLines = $importOptions->getNumberOfIgnoredLines();

        $tablesPrefix = BackendHelper::generateTempTableName();
        $logTable = $tablesPrefix . '_log';
        $logTableQuoted = TeradataQuote::quoteSingleIdentifier($destination->getSchemaName()) . '.' . TeradataQuote::quoteSingleIdentifier($logTable);
        $errTable1 = $tablesPrefix . '_e1';
        $errTableQuoted = TeradataQuote::quoteSingleIdentifier($destination->getSchemaName()) . '.' . TeradataQuote::quoteSingleIdentifier($errTable1);
        $errTable2 = $tablesPrefix . '_e2';
        $errTable2Quoted = TeradataQuote::quoteSingleIdentifier($destination->getSchemaName()) . '.' . TeradataQuote::quoteSingleIdentifier($errTable2);

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
