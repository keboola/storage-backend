<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Exasol;

use Doctrine\DBAL\Exception;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolException;
use Keboola\Db\ImportExport\Storage\FileNotFoundException;
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
}
