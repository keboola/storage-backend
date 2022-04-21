<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Tests\Keboola\Db\ImportExportCommon\StorageType;

class OtherImportTest extends SnowflakeImportExportBaseTest
{
    public function testCopyInvalidSourceDataShouldThrowException(): void
    {
        $options = $this->getSimpleImportOptions();
        $source = new Storage\Snowflake\Table($this->getSourceSchemaName(), 'names', ['c1', 'c2']);
        $destination = new Storage\Snowflake\Table($this->getDestinationSchemaName(), 'out.csv_2Cols');

        self::expectException(Exception::class);
        self::expectExceptionCode(Exception::COLUMNS_COUNT_NOT_MATCH);
        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );
    }

    public function testImportShouldNotFailOnColumnNameRowNumber(): void
    {
        $options = $this->getSimpleImportOptions();
        $source = $this->getSourceInstance(
            'column-name-row-number.csv',
            [
                'id',
                'row_number',
            ]
        );
        $destination = new Storage\Snowflake\Table(
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
        $initialFile = new CsvFile(self::DATA_DIR . 'tw_accounts.csv');
        $options = $this->getSimpleImportOptions();
        $source = $this->getSourceInstance(
            '02_tw_accounts.csv.invalid.manifest',
            $initialFile->getHeader(),
            true
        );
        $destination = new Storage\Snowflake\Table(
            $this->getDestinationSchemaName(),
            'accounts-3'
        );

        $this->expectException(Exception::class);
        if (getenv('STORAGE_TYPE') === StorageType::STORAGE_S3) {
            $this->expectExceptionCode(Exception::INVALID_SOURCE_DATA);
        } else {
            $this->expectExceptionCode(Exception::MANDATORY_FILE_NOT_FOUND);
        }
        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );
    }

    public function testMoreColumnsShouldThrowException(): void
    {
        $options = $this->getSimpleImportOptions();
        $source = $this->getSourceInstance(
            'tw_accounts.csv',
            [
                'first',
                'second',
            ],
            false
        );
        $destination = new Storage\Snowflake\Table(
            $this->getDestinationSchemaName(),
            'out.csv_2Cols'
        );

        self::expectException(Exception::class);
        self::expectExceptionCode(Exception::COLUMNS_COUNT_NOT_MATCH);
        self::expectExceptionMessage('first');
        self::expectExceptionMessage('second');
        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );
    }

    public function testIncrementalImportFromTable(): void
    {
        $fetchSQL = sprintf(
            'SELECT "col1", "col2" FROM "%s"."%s"',
            $this->getDestinationSchemaName(),
            'out.csv_2Cols'
        );

        $source = new Storage\Snowflake\SelectSource(
            sprintf('SELECT * FROM "%s"."%s"', $this->getSourceSchemaName(), 'out.csv_2Cols'),
            [],
            [
                'col1',
                'col2',
            ]
        );
        $destination = new Storage\Snowflake\Table($this->getDestinationSchemaName(), 'out.csv_2Cols');
        $options = $this->getSimpleImportOptions();

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll($fetchSQL);

        $this->assertCount(2, $importedData);

        $this->connection->query(sprintf('INSERT INTO "%s"."out.csv_2Cols" VALUES
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

    public function testNullifyCopy(): void
    {
        $this->connection->query(sprintf(
            'DROP TABLE IF EXISTS "%s"."nullify"',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."nullify" ("id" VARCHAR, "name" VARCHAR, "price" NUMERIC)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'DROP TABLE IF EXISTS "%s"."nullify_src" ',
            $this->getSourceSchemaName()
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."nullify_src" ("id" VARCHAR, "name" VARCHAR, "price" NUMERIC)',
            $this->getSourceSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO "%s"."nullify_src" VALUES(\'1\', \'\', NULL), (\'2\', NULL, 500)',
            $this->getSourceSchemaName()
        ));

        $options = new ImportOptions(
            ['name', 'price'], //convert empty values
            false,
            false,
            ImportOptions::SKIP_FIRST_LINE
        );
        $source = new Storage\Snowflake\Table(
            $this->getSourceSchemaName(),
            'nullify_src',
            [
                'id',
                'name',
                'price',
            ]
        );
        $destination = new Storage\Snowflake\Table($this->getDestinationSchemaName(), 'nullify');

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT "id", "name", "price" FROM "%s"."nullify" ORDER BY "id" ASC',
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
            'DROP TABLE IF EXISTS "%s"."nullify" ',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."nullify" ("id" VARCHAR, "name" VARCHAR, "price" NUMERIC)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO "%s"."nullify" VALUES(\'4\', NULL, 50)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'DROP TABLE IF EXISTS "%s"."nullify_src" ',
            $this->getSourceSchemaName()
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."nullify_src" ("id" VARCHAR, "name" VARCHAR, "price" NUMERIC)',
            $this->getSourceSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO "%s"."nullify_src" VALUES(\'1\', \'\', NULL), (\'2\', NULL, 500)',
            $this->getSourceSchemaName()
        ));

        $options = new ImportOptions(
            ['name', 'price'], //convert empty values
            true, // incremetal
            false,
            ImportOptions::SKIP_FIRST_LINE
        );
        $source = new Storage\Snowflake\Table(
            $this->getSourceSchemaName(),
            'nullify_src',
            [
                'id',
                'name',
                'price',
            ]
        );
        $destination = new Storage\Snowflake\Table($this->getDestinationSchemaName(), 'nullify');

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT "id", "name", "price" FROM "%s"."nullify" ORDER BY "id" ASC',
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
            'DROP TABLE IF EXISTS "%s"."nullify" ',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."nullify" ("id" VARCHAR, "name" VARCHAR, "price" NUMERIC, PRIMARY KEY("id"))',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO "%s"."nullify" VALUES(\'4\', \'3\', 2)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'DROP TABLE IF EXISTS "%s"."nullify_src" ',
            $this->getSourceSchemaName()
        ));
        $this->connection->query(sprintf(
        // phpcs:ignore
            'CREATE TABLE "%s"."nullify_src" ("id" VARCHAR NOT NULL, "name" VARCHAR NOT NULL, "price" VARCHAR NOT NULL, PRIMARY KEY("id"))',
            $this->getSourceSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO "%s"."nullify_src" VALUES(\'1\', \'\', \'\'), (\'2\', \'\', \'500\'), (\'4\', \'\', \'\')',
            $this->getSourceSchemaName()
        ));

        $options = new ImportOptions(
            ['name', 'price'], //convert empty values
            true, // incremetal
            false,
            ImportOptions::SKIP_FIRST_LINE
        );
        $source = new Storage\Snowflake\Table(
            $this->getSourceSchemaName(),
            'nullify_src',
            ['id', 'name', 'price']
        );
        $destination = new Storage\Snowflake\Table(
            $this->getDestinationSchemaName(),
            'nullify'
        );

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT "id", "name", "price" FROM "%s"."nullify"',
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
            'DROP TABLE IF EXISTS "%s"."nullify" ',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."nullify" ("id" VARCHAR, "name" VARCHAR, "price" NUMERIC, PRIMARY KEY("id"))',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO "%s"."nullify" VALUES(\'4\', NULL, NULL)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'DROP TABLE IF EXISTS "%s"."nullify_src" ',
            $this->getSourceSchemaName()
        ));
        $this->connection->query(sprintf(
        // phpcs:ignore
            'CREATE TABLE "%s"."nullify_src" ("id" VARCHAR NOT NULL, "name" VARCHAR NOT NULL, "price" VARCHAR NOT NULL, PRIMARY KEY("id"))',
            $this->getSourceSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO "%s"."nullify_src" VALUES(\'1\', \'\', \'\'), (\'2\', \'\', \'500\'), (\'4\', \'\', \'500\')',
            $this->getSourceSchemaName()
        ));

        $options = new ImportOptions(
            ['name', 'price'], //convert empty values
            true, // incremetal
            false,
            ImportOptions::SKIP_FIRST_LINE
        );
        $source = new Storage\Snowflake\Table(
            $this->getSourceSchemaName(),
            'nullify_src',
            ['id', 'name', 'price']
        );
        $destination = new Storage\Snowflake\Table(
            $this->getDestinationSchemaName(),
            'nullify'
        );

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT "id", "name", "price" FROM "%s"."nullify"',
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
            'DROP TABLE IF EXISTS "%s"."nullify" ',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."nullify" ("id" VARCHAR, "name" VARCHAR, "price" NUMERIC)',
            $this->getDestinationSchemaName()
        ));

        $options = new ImportOptions(
            ['name', 'price'], //convert empty values
            false,
            false,
            ImportOptions::SKIP_FIRST_LINE
        );
        $source = $this->getSourceInstance('nullify.csv', ['id', 'name', 'price']);
        $destination = new Storage\Snowflake\Table(
            $this->getDestinationSchemaName(),
            'nullify'
        );

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT "id", "name", "price" FROM "%s"."nullify" ORDER BY "id" ASC',
            $this->getDestinationSchemaName()
        ));
        $this->assertCount(3, $importedData);
        $this->assertTrue($importedData[1]['name'] === null);
        $this->assertTrue($importedData[2]['price'] === null);
    }

    public function testNullifyCsvIncremental(): void
    {
        $this->connection->query(sprintf(
            'DROP TABLE IF EXISTS "%s"."nullify" ',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."nullify" ("id" VARCHAR, "name" VARCHAR, "price" NUMERIC)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO "%s"."nullify" VALUES(\'4\', NULL, 50)',
            $this->getDestinationSchemaName()
        ));

        $options = new ImportOptions(
            ['name', 'price'], //convert empty values
            true, // incremetal
            false,
            ImportOptions::SKIP_FIRST_LINE
        );
        $source = $this->getSourceInstance('nullify.csv', ['id', 'name', 'price']);
        $destination = new Storage\Snowflake\Table(
            $this->getDestinationSchemaName(),
            'nullify'
        );

        (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );
        $importedData = $this->connection->fetchAll(sprintf(
            'SELECT "id", "name", "price" FROM "%s"."nullify" ORDER BY "id" ASC',
            $this->getDestinationSchemaName()
        ));
        $this->assertCount(4, $importedData);
        $this->assertTrue($importedData[1]['name'] === null);
        $this->assertTrue($importedData[2]['price'] === null);
        $this->assertTrue($importedData[3]['name'] === null);
    }
}
