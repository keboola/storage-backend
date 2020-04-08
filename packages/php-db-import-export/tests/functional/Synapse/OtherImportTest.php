<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Synapse;

use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Backend\Synapse\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;

class OtherImportTest extends SynapseBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema(self::SYNAPSE_DEST_SCHEMA_NAME);
        $this->dropAllWithinSchema(self::SYNAPSE_SOURCE_SCHEMA_NAME);
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', self::SYNAPSE_DEST_SCHEMA_NAME));
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', self::SYNAPSE_SOURCE_SCHEMA_NAME));
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
            new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, self::TABLE_OUT_CSV_2COLS),
            $this->getSimpleImportOptions($escapingHeader)
        );
    }

    public function testCopyInvalidSourceDataShouldThrowException(): void
    {
        $this->initTables([self::TABLE_OUT_CSV_2COLS]);

        $options = $this->getSimpleImportOptions(['c1', 'c2']);
        $source = new Storage\Synapse\Table(self::SYNAPSE_SOURCE_SCHEMA_NAME, 'names');
        $destination = new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, 'out.csv_2Cols');

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

        $options = $this->getSimpleImportOptions([
            'id',
            'row_number',
        ]);
        $source = $this->createABSSourceInstance('column-name-row-number.csv');
        $destination = new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, 'column-name-row-number');

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
        $options = $this->getSimpleImportOptions($initialFile->getHeader());
        $source = $this->createABSSourceInstance('02_tw_accounts.csv.invalid.manifest', true);
        $destination = new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, 'accounts-3');

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
            self::SYNAPSE_DEST_SCHEMA_NAME,
            self::TABLE_OUT_CSV_2COLS
        );

        $source = new Storage\Synapse\SelectSource(
            sprintf('SELECT * FROM [%s].[%s]', self::SYNAPSE_SOURCE_SCHEMA_NAME, self::TABLE_OUT_CSV_2COLS),
            []
        );
        $destination = new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, self::TABLE_OUT_CSV_2COLS);
        $options = $this->getSimpleImportOptions([
            'col1',
            'col2',
        ]);

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll($fetchSQL);

        $this->assertCount(2, $importedData);

        $this->connection->exec(sprintf('INSERT INTO [%s].[out.csv_2Cols] VALUES
                (\'e\', \'f\');
        ', self::SYNAPSE_SOURCE_SCHEMA_NAME));

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

        $options = $this->getSimpleImportOptions([
            'first',
            'second',
        ]);
        $source = $this->createABSSourceInstance('tw_accounts.csv', false);
        $destination = new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, 'out.csv_2Cols');

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
            self::SYNAPSE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE [%s].[nullify_src] ([id] nvarchar(4000), [name] nvarchar(4000), [price] NUMERIC)',
            self::SYNAPSE_SOURCE_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'1\', \'\', NULL)',
            self::SYNAPSE_SOURCE_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'2\', NULL, 500)',
            self::SYNAPSE_SOURCE_SCHEMA_NAME
        ));

        $options = new ImportOptions(
            ['name', 'price'], //convert empty values
            ['id', 'name', 'price'],
            false,
            false,
            ImportOptions::SKIP_FIRST_LINE
        );
        $source = new Storage\Synapse\Table(self::SYNAPSE_SOURCE_SCHEMA_NAME, 'nullify_src');
        $destination = new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, 'nullify');

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT [id], [name], [price] FROM [%s].[nullify] ORDER BY [id] ASC',
            self::SYNAPSE_DEST_SCHEMA_NAME
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
            self::SYNAPSE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify] VALUES(\'4\', NULL, 50)',
            self::SYNAPSE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE [%s].[nullify_src] ([id] nvarchar(4000), [name] nvarchar(4000), [price] NUMERIC)',
            self::SYNAPSE_SOURCE_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'1\', \'\', NULL)',
            self::SYNAPSE_SOURCE_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'2\', NULL, 500)',
            self::SYNAPSE_SOURCE_SCHEMA_NAME
        ));

        $options = new ImportOptions(
            ['name', 'price'], //convert empty values
            ['id', 'name', 'price'],
            true, // incremetal
            false,
            ImportOptions::SKIP_FIRST_LINE
        );
        $source = new Storage\Synapse\Table(self::SYNAPSE_SOURCE_SCHEMA_NAME, 'nullify_src');
        $destination = new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, 'nullify');

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT [id], [name], [price] FROM [%s].[nullify] ORDER BY [id] ASC',
            self::SYNAPSE_DEST_SCHEMA_NAME
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
            self::SYNAPSE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify] VALUES(\'4\', \'3\', 2)',
            self::SYNAPSE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
        // phpcs:ignore
            'CREATE TABLE [%s].[nullify_src] ([id] nvarchar(4000) NOT NULL, [name] nvarchar(4000) NOT NULL, [price] nvarchar(4000) NOT NULL, PRIMARY KEY NONCLUSTERED([id]) NOT ENFORCED)',
            self::SYNAPSE_SOURCE_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'1\', \'\', \'\')',
            self::SYNAPSE_SOURCE_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'2\', \'\', \'500\')',
            self::SYNAPSE_SOURCE_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'4\', \'\', \'\')',
            self::SYNAPSE_SOURCE_SCHEMA_NAME
        ));

        $options = new ImportOptions(
            ['name', 'price'], //convert empty values
            ['id', 'name', 'price'],
            true, // incremetal
            false,
            ImportOptions::SKIP_FIRST_LINE
        );
        $source = new Storage\Synapse\Table(self::SYNAPSE_SOURCE_SCHEMA_NAME, 'nullify_src');
        $destination = new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, 'nullify');

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT [id], [name], [price] FROM [%s].[nullify]',
            self::SYNAPSE_DEST_SCHEMA_NAME
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
            self::SYNAPSE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify] VALUES(\'4\', NULL, NULL)',
            self::SYNAPSE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
        // phpcs:ignore
            'CREATE TABLE [%s].[nullify_src] ([id] nvarchar(4000) NOT NULL, [name] nvarchar(4000) NOT NULL, [price] nvarchar(4000) NOT NULL, PRIMARY KEY NONCLUSTERED([id]) NOT ENFORCED)',
            self::SYNAPSE_SOURCE_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'1\', \'\', \'\')',
            self::SYNAPSE_SOURCE_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'2\', \'\', \'500\')',
            self::SYNAPSE_SOURCE_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'4\', \'\', \'500\')',
            self::SYNAPSE_SOURCE_SCHEMA_NAME
        ));

        $options = new ImportOptions(
            ['name', 'price'], //convert empty values
            ['id', 'name', 'price'],
            true, // incremetal
            false,
            ImportOptions::SKIP_FIRST_LINE
        );
        $source = new Storage\Synapse\Table(self::SYNAPSE_SOURCE_SCHEMA_NAME, 'nullify_src');
        $destination = new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, 'nullify');

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT [id], [name], [price] FROM [%s].[nullify]',
            self::SYNAPSE_DEST_SCHEMA_NAME
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
            self::SYNAPSE_DEST_SCHEMA_NAME
        ));

        $options = new ImportOptions(
            ['name', 'price'], //convert empty values
            ['id', 'name', 'price'],
            false,
            false,
            ImportOptions::SKIP_FIRST_LINE
        );
        $source = $this->createABSSourceInstance('nullify.csv');
        $destination = new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, 'nullify');

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT [id], [name], [price] FROM [%s].[nullify] ORDER BY [id] ASC',
            self::SYNAPSE_DEST_SCHEMA_NAME
        ));
        $this->assertCount(3, $importedData);
        $this->assertTrue($importedData[1]['name'] === null);
        $this->assertTrue($importedData[2]['price'] === null);
    }

    public function testNullifyCsvIncremental(): void
    {
        $this->connection->query(sprintf(
            'CREATE TABLE [%s].[nullify] ([id] nvarchar(4000), [name] nvarchar(4000), [price] NUMERIC)',
            self::SYNAPSE_DEST_SCHEMA_NAME
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify] VALUES(\'4\', NULL, 50)',
            self::SYNAPSE_DEST_SCHEMA_NAME
        ));

        $options = new ImportOptions(
            ['name', 'price'], //convert empty values
            ['id', 'name', 'price'],
            true, // incremetal
            false,
            ImportOptions::SKIP_FIRST_LINE
        );
        $source = $this->createABSSourceInstance('nullify.csv');
        $destination = new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, 'nullify');

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );
        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT [id], [name], [price] FROM [%s].[nullify] ORDER BY [id] ASC',
            self::SYNAPSE_DEST_SCHEMA_NAME
        ));
        $this->assertCount(4, $importedData);
        $this->assertTrue($importedData[1]['name'] === null);
        $this->assertTrue($importedData[2]['price'] === null);
        $this->assertTrue($importedData[3]['name'] === null);
    }
}
