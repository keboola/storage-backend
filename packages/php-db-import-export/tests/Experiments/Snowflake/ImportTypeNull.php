<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportExperiments\Snowflake;

use Doctrine\DBAL\Connection;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\CopyCommandCsvOptionsHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Storage\ABS\BaseFile;
use Keboola\Db\ImportExport\Storage\ABS\SourceFile;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Tests\Keboola\Db\ImportExportCommon\StorageTrait;
use Tests\Keboola\Db\ImportExportCommon\StorageType;
use Tests\Keboola\Db\ImportExportCommon\StubLoader\AbsLoader;
use Tests\Keboola\Db\ImportExportExperiments\BackendHelper;
use Throwable;

#[AsCommand(
    name: 'exp:import-type-null',
    description: 'Test imported empty string using COPY INTO on different types.'
)]
class ImportTypeNull extends Command
{
    use StorageTrait;

    private const SCHEMA = 'expimporttypenull';

    private Connection $db;

    public function __construct()
    {
        parent::__construct('exp:import-type-null');
        $this->db = BackendHelper::getSnowflakeConnection();
    }

    // ...
    protected function configure(): void
    {
        $this
            ->setHelp('This command allows you to create a user...');
    }

    /**
     * @param string[] $options
     */
    private function getCopySQL(string $t, SourceFile $source, array $options, string $file): string
    {
        return sprintf(
            'COPY INTO %s.%s 
FROM %s
CREDENTIALS=(AZURE_SAS_TOKEN=\'%s\')
FILE_FORMAT = (TYPE=CSV %s)
FILES = (%s) ',
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA),
            SnowflakeQuote::quoteSingleIdentifier('ImportTest' . $t),
            QuoteHelper::quote($source->getContainerUrl(BaseFile::PROTOCOL_AZURE)),
            $source->getSasToken(),
            implode(
                ' ',
                [
                    ...CopyCommandCsvOptionsHelper::getCsvCopyCommandOptions(
                        new SnowflakeImportOptions(),
                        $source->getCsvOptions()
                    ),
                    ...$options,
                ]
            ),
            SnowflakeQuote::quote($file)
        );
    }

    protected function initialize(InputInterface $input, OutputInterface $output)
    {
        putenv('STORAGE_TYPE=' . StorageType::STORAGE_ABS);
        putenv('ABS_CONTAINER_NAME=' . self::SCHEMA);
//        $this->db->executeQuery(
//            sprintf(
//                'CREATE OR REPLACE SCHEMA %s;',
//                SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA)
//            )
//        );
//
//        foreach (Snowflake::TYPES as $t) {
//            foreach (['', ' NOT NULL',] as $n) {
//                $this->db->executeQuery(
//                    sprintf(
//                        'CREATE OR REPLACE TABLE %s.%s (%s, "str" TEXT);',
//                        SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA),
//                        SnowflakeQuote::quoteSingleIdentifier('ImportTest' . $t . ($n === ' NOT NULL' ? 'nn' : '')),
//                        sprintf('"col" %s' . $n, $t)
//                    )
//                );
//            }
//        }
//
//        $loader = new AbsLoader(
//            (string) getenv('ABS_ACCOUNT_NAME'),
//            self::SCHEMA
//        );
//        $loader->deleteContainer();
//        $loader->createContainer();
//        echo "Uploading file \n";
////        $loader->getBlobService()->createBlockBlobAsync(
////            self::SCHEMA,
////            'data.csv',
////            '\'\',\'\''
////        )->wait();
//        $loader->getBlobService()->createBlockBlobAsync(
//            self::SCHEMA,
//            'data_empty.csv',
//            ','
//        )->wait();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $table = new Table($output);
        $table
            ->setHeaders(['Type', 'Opts', 'File', 'Status', 'Data', 'Msg']);
        foreach (['data_empty.csv'] as $f) {
            $source = $this->createABSSourceInstance($f);
            foreach (Snowflake::TYPES as $t) {
                foreach (['', ' NOT NULL',] as $n) {
                    foreach ([
                                 [''],
                                 ['NULL_IF=(\'\N\', \'NULL\', \'NUL\', \'\', \'\'\'\')'],
                                 ['EMPTY_FIELD_AS_NULL=true'],
                                 ['NULL_IF=(\'\N\', \'NULL\', \'NUL\', \'\', \'\'\'\')', 'EMPTY_FIELD_AS_NULL=true'],
                             ] as $o
                    ) {
                        $suffix = $t . ($n === ' NOT NULL' ? 'nn' : '');
                        $sql = $this->getCopySQL($suffix, $source, $o, $f);
                        try {
                            $this->db->executeQuery($sql);

                            $table->addRow([
                                $suffix,
                                implode(',', $o),
                                $f,
                                'OK',
                                $this->db->fetchOne(sprintf(
                                    'SELECT "col" FROM %s.%s;',
                                    SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA),
                                    SnowflakeQuote::quoteSingleIdentifier('ImportTest' . $suffix),
                                )),
                                '',
                            ]);
                        } catch (Throwable $e) {
                            $output->writeln('FAIL: ' . $suffix .' '. implode(',', $o) .' '. $f .' <> '. $e->getMessage());
                            $table->addRow([
                                $suffix,
                                implode(',', $o),
                                $f,
                                'FAIL',
                                '',
                                $e->getMessage(),
                            ]);
                        }
                    }
                }
            }
        }
        $table->render();
        return Command::SUCCESS;
    }

    protected
    function getDestinationSchema(): string
    {
        // TODO: Implement getDestinationSchema() method.
    }

    protected
    function getSourceSchema(): string
    {
        // TODO: Implement getSourceSchema() method.
    }
}
