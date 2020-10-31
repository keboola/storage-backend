<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Synapse;

use Doctrine\DBAL\DBALException;
use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Backend\Synapse\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;

class OtherImportTest extends SynapseBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema($this->getDestinationSchemaName());
        $this->dropAllWithinSchema($this->getSourceSchemaName());
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', $this->getDestinationSchemaName()));
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', $this->getSourceSchemaName()));
    }

    public function testInvalidFieldQuote(): void
    {
        $this->initTables([self::TABLE_OUT_CSV_2COLS]);

        $file = new CsvFile(self::DATA_DIR . 'escaping/standard-with-enclosures.csv');
        $expectedEscaping = [];
        foreach ($file as $row) {
            $expectedEscaping[] = $row;
        }
        $escapingHeader = array_shift($expectedEscaping); // remove header

        $this->expectException(\Throwable::class);
        $this->expectExceptionMessage('CSV property FIELDQUOTE|ECLOSURE must be set when using Synapse analytics.');
        (new \Keboola\Db\ImportExport\Backend\Synapse\Importer($this->connection))->importTable(
            $this->createABSSourceInstanceFromCsv('raw.rs.csv', new CsvOptions("\t", '', '\\')),
            new Storage\Synapse\Table($this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS, $escapingHeader),
            $this->getSynapseImportOptions()
        );
    }

    public function testCopyInvalidSourceDataShouldThrowException(): void
    {
        $this->initTables([self::TABLE_OUT_CSV_2COLS]);

        $options = $this->getSynapseImportOptions();
        $source = new Storage\Synapse\Table($this->getSourceSchemaName(), 'names', ['c1', 'c2']);
        $destination = new Storage\Synapse\Table($this->getDestinationSchemaName(), 'out.csv_2Cols');

        $this->expectException(Exception::class);
        $this->expectExceptionCode(Exception::COLUMNS_COUNT_NOT_MATCH);
        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );
    }

    public function testImportShouldNotFailOnColumnNameRowNumber(): void
    {
        $this->initTables([self::TABLE_COLUMN_NAME_ROW_NUMBER]);

        $options = $this->getSynapseImportOptions();
        $source = $this->createABSSourceInstance(
            'column-name-row-number.csv',
            [
                'id',
                'row_number',
            ]
        );
        $destination = new Storage\Synapse\Table(
            $this->getDestinationSchemaName(),
            'column-name-row-number'
        );

        $result = (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );
        self::assertEquals(2, $result->getImportedRowsCount());
    }

    public function testInvalidManifestImport(): void
    {
        $this->initTables([self::TABLE_ACCOUNTS_3]);

        $initialFile = new CsvFile(self::DATA_DIR . 'tw_accounts.csv');
        $options = $this->getSynapseImportOptions();
        $source = $this->createABSSourceInstance(
            '02_tw_accounts.csv.invalid.manifest',
            $initialFile->getHeader(),
            true
        );
        $destination = new Storage\Synapse\Table(
            $this->getDestinationSchemaName(),
            'accounts-3'
        );

        $this->expectException(Exception::class);
        $this->expectExceptionCode(Exception::MANDATORY_FILE_NOT_FOUND);
        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );
    }

    public function testIncrementalImportFromTable(): void
    {
        $this->initTables([self::TABLE_OUT_CSV_2COLS]);
        $fetchSQL = sprintf(
            'SELECT [col1], [col2] FROM [%s].[%s]',
            $this->getDestinationSchemaName(),
            self::TABLE_OUT_CSV_2COLS
        );

        $source = new Storage\Synapse\SelectSource(
            sprintf('SELECT * FROM [%s].[%s]', $this->getSourceSchemaName(), self::TABLE_OUT_CSV_2COLS),
            [],
            [],
            [
                'col1',
                'col2',
            ]
        );
        $destination = new Storage\Synapse\Table($this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS);
        $options = $this->getSynapseImportOptions();

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll($fetchSQL);

        $this->assertCount(2, $importedData);

        $this->connection->exec(sprintf('INSERT INTO [%s].[out.csv_2Cols] VALUES
                (\'e\', \'f\');
        ', $this->getSourceSchemaName()));

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll($fetchSQL);

        $this->assertCount(3, $importedData);
    }

    public function testMoreColumnsShouldThrowException(): void
    {
        $this->initTables([self::TABLE_OUT_CSV_2COLS]);

        $options = $this->getSynapseImportOptions();
        $source = $this->createABSSourceInstance(
            'tw_accounts.csv',
            [
                'first',
                'second',
            ],
            false
        );
        $destination = new Storage\Synapse\Table(
            $this->getDestinationSchemaName(),
            'out.csv_2Cols'
        );

        $this->expectException(Exception::class);
        $this->expectExceptionCode(Exception::COLUMNS_COUNT_NOT_MATCH);
        $this->expectExceptionMessage('first');
        $this->expectExceptionMessage('second');
        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );
    }

    public function testNullifyCopy(): void
    {
        $this->connection->query(sprintf(
            'CREATE TABLE [%s].[nullify] ([id] nvarchar(4000), [name] nvarchar(4000), [price] NUMERIC)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE [%s].[nullify_src] ([id] nvarchar(4000), [name] nvarchar(4000), [price] NUMERIC)',
            $this->getSourceSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'1\', \'\', NULL)',
            $this->getSourceSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'2\', NULL, 500)',
            $this->getSourceSchemaName()
        ));

        $options = new SynapseImportOptions(
            ['name', 'price'], //convert empty values
            false,
            false,
            ImportOptions::SKIP_FIRST_LINE,
            getenv('CREDENTIALS_IMPORT_TYPE'),
            getenv('TEMP_TABLE_TYPE')
        );
        $source = new Storage\Synapse\Table($this->getSourceSchemaName(), 'nullify_src', ['id', 'name', 'price']);
        $destination = new Storage\Synapse\Table(
            $this->getDestinationSchemaName(),
            'nullify'
        );

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT [id], [name], [price] FROM [%s].[nullify] ORDER BY [id] ASC',
            $this->getDestinationSchemaName()
        ));
        $this->assertCount(2, $importedData);
        $this->assertTrue($importedData[0]['name'] === null);
        $this->assertTrue($importedData[0]['price'] === null);
        $this->assertTrue($importedData[1]['name'] === null);
    }

    public function testNullifyCopyIncremental(): void
    {
        $this->connection->query(sprintf(
            'CREATE TABLE [%s].[nullify] ([id] nvarchar(4000), [name] nvarchar(4000), [price] NUMERIC)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify] VALUES(\'4\', NULL, 50)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE [%s].[nullify_src] ([id] nvarchar(4000), [name] nvarchar(4000), [price] NUMERIC)',
            $this->getSourceSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'1\', \'\', NULL)',
            $this->getSourceSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'2\', NULL, 500)',
            $this->getSourceSchemaName()
        ));

        $options = new SynapseImportOptions(
            ['name', 'price'], //convert empty values
            true, // incremetal
            false,
            ImportOptions::SKIP_FIRST_LINE,
            getenv('CREDENTIALS_IMPORT_TYPE'),
            getenv('TEMP_TABLE_TYPE')
        );
        $source = new Storage\Synapse\Table($this->getSourceSchemaName(), 'nullify_src', ['id', 'name', 'price']);
        $destination = new Storage\Synapse\Table($this->getDestinationSchemaName(), 'nullify');

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT [id], [name], [price] FROM [%s].[nullify] ORDER BY [id] ASC',
            $this->getDestinationSchemaName()
        ));
        $this->assertCount(3, $importedData);
        $this->assertTrue($importedData[0]['name'] === null);
        $this->assertTrue($importedData[0]['price'] === null);
        $this->assertTrue($importedData[1]['name'] === null);
        $this->assertTrue($importedData[2]['name'] === null);
    }

    public function testNullifyCopyIncrementalWithPk(): void
    {
        $this->connection->query(sprintf(
        // phpcs:ignore
            'CREATE TABLE [%s].[nullify] ([id] nvarchar(4000), [name] nvarchar(4000), [price] NUMERIC, PRIMARY KEY NONCLUSTERED([id]) NOT ENFORCED)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify] VALUES(\'4\', \'3\', 2)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
        // phpcs:ignore
            'CREATE TABLE [%s].[nullify_src] ([id] nvarchar(4000) NOT NULL, [name] nvarchar(4000) NOT NULL, [price] nvarchar(4000) NOT NULL, PRIMARY KEY NONCLUSTERED([id]) NOT ENFORCED)',
            $this->getSourceSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'1\', \'\', \'\')',
            $this->getSourceSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'2\', \'\', \'500\')',
            $this->getSourceSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'4\', \'\', \'\')',
            $this->getSourceSchemaName()
        ));

        $options = new SynapseImportOptions(
            ['name', 'price'], //convert empty values
            true, // incremetal
            false,
            ImportOptions::SKIP_FIRST_LINE
        );
        $source = new Storage\Synapse\Table($this->getSourceSchemaName(), 'nullify_src', ['id', 'name', 'price']);
        $destination = new Storage\Synapse\Table(
            $this->getDestinationSchemaName(),
            'nullify'
        );

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT [id], [name], [price] FROM [%s].[nullify]',
            $this->getDestinationSchemaName()
        ));
        $expectedData = [
            [
                'id' => '1',
                'name' => null,
                'price' => null,
            ],
            [
                'id' => '2',
                'name' => null,
                'price' => '500',
            ],
            [
                'id' => '4',
                'name' => null,
                'price' => null,
            ],

        ];

        $this->assertArrayEqualsSorted($expectedData, $importedData, 'id');
    }

    public function testNullifyCopyIncrementalWithPkDestinationWithNull(): void
    {
        $this->connection->query(sprintf(
        // phpcs:ignore
            'CREATE TABLE [%s].[nullify] ([id] nvarchar(4000), [name] nvarchar(4000), [price] NUMERIC, PRIMARY KEY NONCLUSTERED([id]) NOT ENFORCED)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify] VALUES(\'4\', NULL, NULL)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
        // phpcs:ignore
            'CREATE TABLE [%s].[nullify_src] ([id] nvarchar(4000) NOT NULL, [name] nvarchar(4000) NOT NULL, [price] nvarchar(4000) NOT NULL, PRIMARY KEY NONCLUSTERED([id]) NOT ENFORCED)',
            $this->getSourceSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'1\', \'\', \'\')',
            $this->getSourceSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'2\', \'\', \'500\')',
            $this->getSourceSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'4\', \'\', \'500\')',
            $this->getSourceSchemaName()
        ));

        $options = new SynapseImportOptions(
            ['name', 'price'], //convert empty values
            true, // incremetal
            false,
            ImportOptions::SKIP_FIRST_LINE,
            getenv('CREDENTIALS_IMPORT_TYPE'),
            getenv('TEMP_TABLE_TYPE')
        );
        $source = new Storage\Synapse\Table($this->getSourceSchemaName(), 'nullify_src', ['id', 'name', 'price']);
        $destination = new Storage\Synapse\Table(
            $this->getDestinationSchemaName(),
            'nullify'
        );

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT [id], [name], [price] FROM [%s].[nullify]',
            $this->getDestinationSchemaName()
        ));
        $expectedData = [
            [
                'id' => '1',
                'name' => null,
                'price' => null,
            ],
            [
                'id' => '2',
                'name' => null,
                'price' => '500',
            ],
            [
                'id' => '4',
                'name' => null,
                'price' => '500',
            ],

        ];

        $this->assertArrayEqualsSorted($expectedData, $importedData, 'id');
    }

    public function testNullifyCsv(): void
    {
        $this->connection->query(sprintf(
            'CREATE TABLE [%s].[nullify] ([id] nvarchar(4000), [name] nvarchar(4000), [price] NUMERIC)',
            $this->getDestinationSchemaName()
        ));

        $options = new SynapseImportOptions(
            ['name', 'price'], //convert empty values
            false,
            false,
            ImportOptions::SKIP_FIRST_LINE,
            getenv('CREDENTIALS_IMPORT_TYPE'),
            getenv('TEMP_TABLE_TYPE')
        );
        $source = $this->createABSSourceInstance('nullify.csv', ['id', 'name', 'price']);
        $destination = new Storage\Synapse\Table(
            $this->getDestinationSchemaName(),
            'nullify'
        );

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT [id], [name], [price] FROM [%s].[nullify] ORDER BY [id] ASC',
            $this->getDestinationSchemaName()
        ));
        $this->assertCount(3, $importedData);
        $this->assertTrue($importedData[1]['name'] === null);
        $this->assertTrue($importedData[2]['price'] === null);
    }

    public function testNullifyCsvIncremental(): void
    {
        $this->connection->query(sprintf(
            'CREATE TABLE [%s].[nullify] ([id] nvarchar(4000), [name] nvarchar(4000), [price] NUMERIC)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify] VALUES(\'4\', NULL, 50)',
            $this->getDestinationSchemaName()
        ));

        $options = new SynapseImportOptions(
            ['name', 'price'], //convert empty values
            true, // incremetal
            false,
            ImportOptions::SKIP_FIRST_LINE,
            getenv('CREDENTIALS_IMPORT_TYPE'),
            getenv('TEMP_TABLE_TYPE')
        );
        $source = $this->createABSSourceInstance('nullify.csv', ['id', 'name', 'price']);
        $destination = new Storage\Synapse\Table(
            $this->getDestinationSchemaName(),
            'nullify'
        );

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );
        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT [id], [name], [price] FROM [%s].[nullify] ORDER BY [id] ASC',
            $this->getDestinationSchemaName()
        ));
        $this->assertCount(4, $importedData);
        $this->assertTrue($importedData[1]['name'] === null);
        $this->assertTrue($importedData[2]['price'] === null);
        $this->assertTrue($importedData[3]['name'] === null);
    }

    public function testLongColumnImport(): void
    {
        $this->initTables([self::TABLE_OUT_CSV_2COLS]);

        if (getenv('TEMP_TABLE_TYPE') === SynapseImportOptions::TEMP_TABLE_COLUMNSTORE) {
            $this->expectException(DBALException::class);
            $this->expectExceptionMessage(
                '[Microsoft][ODBC Driver 17 for SQL Server][SQL Server]Bulk load data conversion error'
            );
        }

        (new \Keboola\Db\ImportExport\Backend\Synapse\Importer($this->connection))->importTable(
            $this->createABSSourceInstanceFromCsv('long_col.csv', new CsvOptions(), [
                'col1',
                'col2',
            ]),
            $table = new Storage\Synapse\Table($this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS, [
                'col1',
                'col2',
            ]),
            $this->getSynapseImportOptions()
        );

        if (getenv('TEMP_TABLE_TYPE') === SynapseImportOptions::TEMP_TABLE_HEAP) {
            $sql = sprintf(
                'SELECT [col1],[col2] FROM %s',
                $table->getQuotedTableWithScheme()
            );
            $queryResult = array_map(function ($row) {
                return array_map(function ($column) {
                    return $column;
                }, array_values($row));
            }, $this->connection->fetchAll($sql));

            $this->assertEquals(4000, strlen($queryResult[0][0]));
        }
    }
}
