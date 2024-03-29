<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Exasol;

use Doctrine\DBAL\Exception;
use Keboola\Db\Import\Exception as DBException;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolException;
use Keboola\Db\ImportExport\Storage\FileNotFoundException;
use Keboola\Db\ImportExport\Storage\InvalidSourceDataException;
use PHPUnit\Framework\TestCase;

class ExasolExceptionTest extends TestCase
{
    public function testFileNotFound(): void
    {
        // @codingStandardsIgnoreStart
        $exceptionMessage = <<<EOD
An exception occurred while executing '
IMPORT INTO "some_tests-tests-exasol"."__temp_csvimport6124ae6f501851_90185437" FROM CSV AT 'https://zajca-php-db-import-test-s3filesbucket-bwdj3sk0c9xy.s3.eu-central-1.amazonaws.com'
USER '<IAM ROLE>' IDENTIFIED BY '<IAM Secret>'
FILE 'not-exists.csv' --- files
--- file_opt
SKIP=1
COLUMN SEPARATOR=','
COLUMN DELIMITER='"'
':

SQLSTATE[42636]: <<Unknown error>>: -6819666 [EXASOL][EXASolution driver]ETL-5105: Following error occured while reading data from external connection [https://zajca-php-db-import-test-s3filesbucket-bwdj3sk0c9xy.s3.eu-central-1.amazonaws.com/not-exists.csv failed with error code=404 after 285 bytes. NoSuchKey: The specified key does not exist.] (Session: 1708962746503790595) (SQLExecDirect[4288147630] at /usr/src/php/ext/pdo_odbc/odbc_driver.c:246)
EOD;
        // @codingStandardsIgnoreEnd
        $exception = ExasolException::covertException(new Exception($exceptionMessage));

        $this->assertInstanceOf(FileNotFoundException::class, $exception);
        // phpcs:ignore
        $this->assertSame('Load error: Following error occured while reading data from external connection [https://zajca-php-db-import-test-s3filesbucket-bwdj3sk0c9xy.s3.eu-central-1.amazonaws.com/not-exists.csv failed with error code=404 after 285 bytes. NoSuchKey: The specified key does not exist.]', $exception->getMessage());
    }

    public function testInvalidFileContents(): void
    {
        // @codingStandardsIgnoreStart
        $exceptionMessage = <<<EOD
An exception occurred while executing '
IMPORT INTO "some_tests-tests-exasol"."__temp_csvimport6124ae6f501851_90185437" FROM CSV AT 'https://zajca-php-db-import-test-s3filesbucket-bwdj3sk0c9xy.s3.eu-central-1.amazonaws.com'
USER '<IAM ROLE>' IDENTIFIED BY '<IAM Secret>'
FILE 'somestuff/439058392.gz.test.csv.csv.gz' --- files
--- file_opt

COLUMN SEPARATOR='|'
COLUMN DELIMITER='"'
':

SQLSTATE[42636]: <<Unknown error>>: -6819666 [EXASOL][EXASolution driver]ETL-2105: Error while parsing row=0 (starting from 0) [CSV Parser found at byte 19 (starting with 0 at the beginning of the row) of 19 a single field delimiter in enclosed field or not correct enclosed field in file 'somestuff/some.csv.csv.gz'. Please check for unescaped field delimiters in data fields (they have to be escaped) and correct enclosing of this field] (Session: 1737613353456720129) (SQLExecDirect[12345678] at /usr/src/php/ext/pdo_odbc/odbc_driver.c:246)
EOD;
        // @codingStandardsIgnoreEnd
        $exception = ExasolException::covertException(new Exception($exceptionMessage));

        $this->assertInstanceOf(InvalidSourceDataException::class, $exception);
        // phpcs:ignore
        $this->assertSame('Load error: Error while parsing row=0 (starting from 0) [CSV Parser found at byte 19 (starting with 0 at the beginning of the row) of 19 a single field delimiter in enclosed field or not correct enclosed field in file \'somestuff/some.csv.csv.gz\'. Please check for unescaped field delimiters in data fields (they have to be escaped) and correct enclosing of this field]', $exception->getMessage());
    }

    public function testConstraintViolationNotNull(): void
    {
        // @codingStandardsIgnoreStart
        $exceptionMessage = <<<EOD
An exception occurred while executing 'INSERT INTO "in_c-API-tests-b9e1dc60f915aeb6aaf95ed032cab50f67299b50"."__temp_DEDUP_csvimport6172585e9775b1_40593964" ("id", "name") SELECT a."id",a."name" FROM (SELECT "id", "name", ROW_NUMBER() OVER (PARTITION BY "name" ORDER BY "name") AS "_row_number_" FROM "DEV_ZAJCA_1594-in_c-API-tests-b9e1dc60f915aeb6aaf95ed032cab50f67299b50"."__temp_csvimport6172585dbcdcb3_59853165") AS a WHERE a."_row_number_" = 1':

SQLSTATE[27001]: <<Unknown error>>: -3685825 [EXASOL][EXASolution driver]constraint violation - not null (column name in table __temp_DEDUP_csvimport6172585e9775b1_40593964) (Session: 1714299785649455106) (SQLExecDirect[4291281471] at /usr/src/php/ext/pdo_odbc/odbc_driver.c:246)
EOD;
        // @codingStandardsIgnoreEnd
        $exception = ExasolException::covertException(new Exception($exceptionMessage));

        $this->assertInstanceOf(DBException::class, $exception);
        // phpcs:ignore
        $this->assertSame('Load error: Constraint violation - not null (column name).', $exception->getMessage());
    }
}
