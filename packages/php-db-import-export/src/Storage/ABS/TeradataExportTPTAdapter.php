<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\Exception\FailedTPTLoadException;
use Keboola\Db\ImportExport\ExportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\ABS\DestinationFile;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;
use Keboola\Db\ImportExport\Storage\Teradata\TeradataExportOptions;
use Keboola\Temp\Temp;
use RuntimeException;
use Symfony\Component\Process\Process;

class TeradataExportTPTAdapter implements BackendExportAdapterInterface
{
    private const TPT_TIMEOUT = 60 * 60 * 6; // 6 hours

    public static function isSupported(Storage\SourceInterface $source, Storage\DestinationInterface $destination): bool
    {
        if (!$source instanceof Storage\SqlSourceInterface) {
            return false;
        }
        if (!$destination instanceof Storage\ABS\DestinationFile) {
            return false;
        }
        return true;
    }

    /**
     * @return array<mixed>
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ExportOptionsInterface $exportOptions
    ): array {
        assert($source instanceof Storage\SqlSourceInterface);
        assert($destination instanceof DestinationFile);
        assert($exportOptions instanceof TeradataExportOptions);

        /**
         * @var Temp $temp
         */
        [
            $temp,
            $processCmd,
        ] = $this->generateTPTExportScript($source, $destination, $exportOptions);

        $process = new Process(
            $processCmd,
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

        if ($process->getExitCode() !== 0) {
            throw new FailedTPTLoadException(
                $process->getErrorOutput(),
                $process->getOutput(),
                $process->getExitCode(),
                $this->getLogData($temp),
            );
        }

        // delete temp files
        $temp->remove();

        if ($exportOptions->generateManifest()) {
            (new Storage\ABS\ManifestGenerator\AbsSlicedManifestFromFolderGenerator($destination->getClient()))
                ->generateAndSaveManifest($destination->getRelativePath());
        }

        return [];
    }

    /**
     * generates params to run TPT script
     *
     * @return array{Temp, array<int, string>}
     */
    private function generateTPTExportScript(
        SqlSourceInterface $source,
        DestinationFile $destination,
        TeradataExportOptions $exportOptions
    ): array {
        $temp = new Temp();
        $folder = $temp->getTmpFolder();
        $absConfigDir = $folder . '/.abs';
        if (!mkdir($absConfigDir) && !is_dir($absConfigDir)) {
            throw new RuntimeException(sprintf('Directory "%s" was not created', $absConfigDir));
        }
        $credentials = sprintf(
            <<<INI
            [default]
            StorageAccountName = %s
            StorageAccountKey = %s
            INI,
            $destination->getAccountName(),
            $destination->getBlobMasterKey(),
        );
        file_put_contents($absConfigDir . '/credentials', $credentials);

        $path = $destination->getRelativePath();

        $tptScript = sprintf(
            /** @lang SQL */<<<EOD
USING CHARACTER SET UTF8
DEFINE JOB EXPORT_FROM_TERADATA
DESCRIPTION 'Export data from Teradata to Microsoft Azure Blob Storage'
(
    STEP EXPORT_THE_DATA
    (
        APPLY TO OPERATOR ( \$FILE_WRITER()
            ATTR
            (
                AccessModuleName = 'libazureaxsmod.so',
                AccessModuleInitStr = '-ConfigDir "%s" -Container "%s" -Prefix "%s/" -Object "%s" %s'
            )
        )
        SELECT * FROM OPERATOR ( \$EXPORT
            ATTR
            (
                SelectStmt = '%s'
            )
        );
    );
);
EOD,
            $absConfigDir,
            $destination->getContainer(),
            $path->getPathWithoutRoot(),
            $path->getFileName() . ($exportOptions->isCompressed() ? '.gz' : ''),
            $exportOptions->generateABSSizeOptions(),
            $source->getFromStatement()
        );

        $jobVariableFile = sprintf(
            <<<EOD
/********************************************************/
/* TPT attributes - Common for all Samples */
/********************************************************/
TargetTdpId = '%s'
,TargetUserName = '%s'
,TargetUserPassword = '%s'
,TargetErrorList = [ '3706','3803','3807' ]
,DDLPrivateLogName = 'DDL_OPERATOR_LOG'
/********************************************************/
/* TPT EXPORT Operator attributes */
/********************************************************/
,ExportPrivateLogName = 'EXPORT_OPERATOR_LOG'
,SourceTdpId = '%s'
,SourceUserName = '%s'
,SourceUserPassword ='%s'
/********************************************************/
/* TPT DataConnector Consumer Operator */
/********************************************************/
,FileWriterFormat = 'Delimited'
,FileWriterTextDelimiter = ','
,FileWriterEscapeTextDelimiter = '\'
,FileWriterQuotedData = 'Y'
,FileWriterOpenQuoteMark = '"'
,FileWriterCloseQuoteMark = '"'
,FileWriterPrivateLogName = 'FILE_WRITER_LOG'
,FileWriterOpenMode = 'Write'
/********************************************************/
/* APPLY STATEMENT parameters */
/********************************************************/
,ExportInstances = 1
,FileWriterInstances = 1
EOD,
            ...$exportOptions->getTeradataCredentials(),
            ...$exportOptions->getTeradataCredentials(),
        );

        file_put_contents($folder . '/export_script.tpt', $tptScript);
        file_put_contents($folder . '/export_vars.txt', $jobVariableFile);

        return [
            $temp,
            [
                'tbuild',
                '-j',
                'export',
                '-L',
                $folder,
                '-f',
                $folder . '/export_script.tpt',
                '-v',
                $folder . '/export_vars.txt',
            ],
        ];
    }

    private function getLogData(Temp $temp): string
    {
        if (file_exists($temp->getTmpFolder() . '/export-1.out')) {
            $data = file_get_contents($temp->getTmpFolder() . '/export-1.out') ?: 'unable to get error';
            // delete temp files
            $temp->remove();
            return $data;
        }

        return 'unable to get error';
    }
}
