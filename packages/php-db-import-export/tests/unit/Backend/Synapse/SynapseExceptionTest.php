<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Synapse;

use Doctrine\DBAL\Exception as DBALException;
use Keboola\Db\Import\Exception as LibException;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseException;
use PHPUnit\Framework\TestCase;

class SynapseExceptionTest extends TestCase
{
    public function testConvertExceptionBulkDataLoad(): void
    {
        // phpcs:ignore
        $originalMessage = 'An exception occurred while executing \'COPY INTO [xxxx] [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]Bulk load data conversion error (type mismatch or invalid character for the specified codepage) for row 2, column 1 (col1) in data file /long_col_6k.csv.';
        // phpcs:ignore
        $expectedMessage = '[SQL Server]Bulk load data conversion error (type mismatch or invalid character for the specified codepage) for row 2, column 1 (col1) in data file /long_col_6k.csv.';
        $dbalException = new DBALException($originalMessage);

        $resultingException = SynapseException::covertException($dbalException);
        self::assertInstanceOf(LibException::class, $resultingException);
        self::assertSame($expectedMessage, $resultingException->getMessage());
    }

    public function testNoConvert(): void
    {
        $expectedMessage = 'Some random database error.';
        $dbalException = new DBALException($expectedMessage);

        $resultingException = SynapseException::covertException($dbalException);
        self::assertInstanceOf(DBALException::class, $resultingException);
        self::assertSame($expectedMessage, $resultingException->getMessage());
    }

    public function testConvertExceptionDataConversion(): void
    {
        // phpcs:ignore
        $originalMessage = 'An exception occurred while executing \'INSERT INTO SQLSTATE[42000]: [Microsoft][ODBC Driver 17 for SQL Server][SQL Server][Microsoft][ODBC Driver 17 for SQL Server][SQL Server]Error converting data type nvarchar to numeric.';
        $expectedMessage = '[SQL Server]Error converting data type nvarchar to numeric.';
        $dbalException = new DBALException($originalMessage);

        $resultingException = SynapseException::covertException($dbalException);
        self::assertInstanceOf(LibException::class, $resultingException);
        self::assertSame($expectedMessage, $resultingException->getMessage());
    }
}
