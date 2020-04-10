<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake;

use Keboola\Db\ImportExport\Backend\Snowflake\SqlCommandBuilder;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\Snowflake\Table;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use PHPUnit\Framework\TestCase;

class SqlCommandBuilderTest extends TestCase
{
    public function testGetBeginTransaction(): void
    {
        $sql = $this->getInstance()->getBeginTransaction();
        self::assertEquals('BEGIN TRANSACTION', $sql);
    }

    private function getInstance(): SqlCommandBuilder
    {
        return new SqlCommandBuilder();
    }

    public function testGetCommitTransaction(): void
    {
        $sql = $this->getInstance()->getCommitTransaction();
        self::assertEquals('COMMIT', $sql);
    }

    public function testGetCreateStagingTableCommand(): void
    {
        $sql = $this->getInstance()->getCreateStagingTableCommand('schema', 'stagingTable', [
            'col1',
            'col2',
        ]);
        self::assertEquals('CREATE TEMPORARY TABLE "schema"."stagingTable" ("col1" varchar, "col2" varchar)', $sql);
    }

    public function testGetDedupCommand(): void
    {
        $sql = $this->getInstance()->getDedupCommand(
            $this->getDummySource(),
            $this->getDummyTableDestination(),
            [
                'pk1',
                'pk2',
            ],
            'stagingTable',
            'tempTable'
        );
        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "schema"."tempTable" ("col1", "col2") SELECT a."col1",a."col2" FROM (SELECT "col1", "col2", ROW_NUMBER() OVER (PARTITION BY "pk1","pk2" ORDER BY "pk1","pk2") AS "_row_number_"FROM "schema"."stagingTable") AS a WHERE a."_row_number_" = 1',
            $sql
        );
    }

    private function getDummyImportOptions(): ImportOptions
    {
        return new ImportOptions([]);
    }

    private function getDummySource(): SourceInterface
    {
        return new class implements SourceInterface {
            public function getColumnsNames(): array
            {
                return ['col1', 'col2'];
            }
        };
    }

    private function getDummyTableDestination(): Table
    {
        return new Table('schema', 'table');
    }

    public function testGetDedupCommandNoPrimaryKeys(): void
    {
        $sql = $this->getInstance()->getDedupCommand(
            $this->getDummySource(),
            $this->getDummyTableDestination(),
            [],
            'stagingTable',
            'tempTable'
        );
        self::assertEquals('', $sql);
    }

    public function testGetDeleteOldItemsCommand(): void
    {
        $sql = $this->getInstance()->getDeleteOldItemsCommand(
            $this->getDummyTableDestination(),
            'stagingTable',
            [
                'pk1',
                'pk2',
            ]
        );
        self::assertEquals(
        // phpcs:ignore
            'DELETE FROM "schema"."stagingTable" "src" USING "schema"."table" AS "dest" WHERE "dest"."pk1" = COALESCE("src"."pk1", \'\') AND "dest"."pk2" = COALESCE("src"."pk2", \'\') ',
            $sql
        );
    }

    public function testGetDropCommand(): void
    {
        $sql = $this->getInstance()->getDropCommand('schema', 'table');
        self::assertEquals('DROP TABLE "schema"."table"', $sql);
    }

    public function testGetInsertAllIntoTargetTableCommand(): void
    {
        // no convert values no timestamp
        $sql = $this->getInstance()->getInsertAllIntoTargetTableCommand(
            $this->getDummySource(),
            $this->getDummyTableDestination(),
            $this->getDummyImportOptions(),
            'staging table'
        );
        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "schema"."table" ("col1", "col2") (SELECT COALESCE("col1", \'\') AS "col1", COALESCE("col2", \'\') AS "col2" FROM "schema"."staging table")',
            $sql
        );

        // converver values
        $options = new ImportOptions(['col1']);
        $sql = $this->getInstance()->getInsertAllIntoTargetTableCommand(
            $this->getDummySource(),
            $this->getDummyTableDestination(),
            $options,
            'staging table'
        );
        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "schema"."table" ("col1", "col2") (SELECT IFF("col1" = \'\', NULL, "col1"), COALESCE("col2", \'\') AS "col2" FROM "schema"."staging table")',
            $sql
        );

        // use timestamp
        $options = new ImportOptions(['col1'], false, true);
        $sql = $this->getInstance()->getInsertAllIntoTargetTableCommand(
            $this->getDummySource(),
            $this->getDummyTableDestination(),
            $options,
            'staging table'
        );
        self::assertStringStartsWith(
        // phpcs:ignore
            'INSERT INTO "schema"."table" ("col1", "col2", "_timestamp") (SELECT IFF("col1" = \'\', NULL, "col1"), COALESCE("col2", \'\') AS "col2", \'',
            $sql
        );
        // there is datetime between
        self::assertStringEndsWith(
            '\' FROM "schema"."staging table")',
            $sql
        );
    }

    public function testGetInsertFromStagingToTargetTableCommand(): void
    {
        $options = new ImportOptions(['col1'], false, false);
        $sql = $this->getInstance()->getInsertFromStagingToTargetTableCommand(
            $this->getDummySource(),
            $this->getDummyTableDestination(),
            $options,
            'stagingTable',
            '2020-01-01 00:00:00'
        );
        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "schema"."table" ("col1", "col2") SELECT IFF("src"."col1" = \'\', NULL, "col1"),COALESCE("src"."col2", \'\') FROM "schema"."stagingTable" AS "src"',
            $sql
        );
        $options = new ImportOptions(['col1'], false, true);
        $sql = $this->getInstance()->getInsertFromStagingToTargetTableCommand(
            $this->getDummySource(),
            $this->getDummyTableDestination(),
            $options,
            'stagingTable',
            '2020-01-01 00:00:00'
        );
        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "schema"."table" ("col1", "col2", "_timestamp") SELECT IFF("src"."col1" = \'\', NULL, "col1"),COALESCE("src"."col2", \'\'),\'2020-01-01 00:00:00\' FROM "schema"."stagingTable" AS "src"',
            $sql
        );
    }

    public function testGetRenameTableCommand(): void
    {
        $sql = $this->getInstance()->getRenameTableCommand('schema', 'sourceTable', 'targetTable');
        self::assertEquals('ALTER TABLE "schema"."sourceTable" RENAME TO "schema"."targetTable"', $sql);
    }

    public function testGetTableItemsCountCommand(): void
    {
        $sql = $this->getInstance()->getTableItemsCountCommand('schema', 'table');
        self::assertEquals('SELECT COUNT(*) AS "count" FROM "schema"."table"', $sql);
    }

    public function testGetTruncateTableCommand(): void
    {
        $sql = $this->getInstance()->getTruncateTableCommand('schema', 'table');
        self::assertEquals('TRUNCATE "schema"."table"', $sql);
    }

    public function testGetUpdateWithPkCommand(): void
    {
        // no convert values no timestamp
        $sql = $this->getInstance()->getUpdateWithPkCommand(
            $this->getDummySource(),
            $this->getDummyTableDestination(),
            $this->getDummyImportOptions(),
            'staging table',
            ['col1'],
            '2020-01-01 00:00:00'
        );
        self::assertEquals(
        // phpcs:ignore
            'UPDATE "schema"."table" AS "dest" SET "col1" = COALESCE("src"."col1", \'\'), "col2" = COALESCE("src"."col2", \'\') FROM "schema"."staging table" AS "src" WHERE "dest"."col1" = COALESCE("src"."col1", \'\')  AND (COALESCE(TO_VARCHAR("dest"."col1"), \'\') != COALESCE("src"."col1", \'\') OR COALESCE(TO_VARCHAR("dest"."col2"), \'\') != COALESCE("src"."col2", \'\')) ',
            $sql
        );

        // converver values
        $options = new ImportOptions(['col1']);
        $sql = $this->getInstance()->getUpdateWithPkCommand(
            $this->getDummySource(),
            $this->getDummyTableDestination(),
            $options,
            'staging table',
            ['col1'],
            '2020-01-01 00:00:00'
        );
        self::assertEquals(
        // phpcs:ignore
            'UPDATE "schema"."table" AS "dest" SET "col1" = IFF("src"."col1" = \'\', NULL, "src"."col1"), "col2" = COALESCE("src"."col2", \'\') FROM "schema"."staging table" AS "src" WHERE "dest"."col1" = COALESCE("src"."col1", \'\')  AND (COALESCE(TO_VARCHAR("dest"."col1"), \'\') != COALESCE("src"."col1", \'\') OR COALESCE(TO_VARCHAR("dest"."col2"), \'\') != COALESCE("src"."col2", \'\')) ',
            $sql
        );

        // use timestamp
        $options = new ImportOptions(['col1'], false, true);
        $sql = $this->getInstance()->getUpdateWithPkCommand(
            $this->getDummySource(),
            $this->getDummyTableDestination(),
            $options,
            'staging table',
            ['col1'],
            '2020-01-01 00:00:00'
        );

        self::assertEquals(
        // phpcs:ignore
            'UPDATE "schema"."table" AS "dest" SET "col1" = IFF("src"."col1" = \'\', NULL, "src"."col1"), "col2" = COALESCE("src"."col2", \'\'), "_timestamp" = \'2020-01-01 00:00:00\' FROM "schema"."staging table" AS "src" WHERE "dest"."col1" = COALESCE("src"."col1", \'\')  AND (COALESCE(TO_VARCHAR("dest"."col1"), \'\') != COALESCE("src"."col1", \'\') OR COALESCE(TO_VARCHAR("dest"."col2"), \'\') != COALESCE("src"."col2", \'\')) ',
            $sql
        );
    }
}
