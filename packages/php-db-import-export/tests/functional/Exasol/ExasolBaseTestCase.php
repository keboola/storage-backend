<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Exasol;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Keboola\Db\ImportExport\Backend\Synapse\SqlCommandBuilder;
use Keboola\TableBackendUtils\Connection\Exasol\ExasolConnection;
use Tests\Keboola\Db\ImportExportFunctional\ImportExportBaseTest;

class ExasolBaseTestCase extends ImportExportBaseTest
{
    protected const EXASOL_DEST_SCHEMA_NAME = 'in.c-tests';
    protected const EXASOL_SOURCE_SCHEMA_NAME = 'some.tests';

    public const TABLE_ACCOUNTS_3 = 'accounts-3';
    public const TABLE_ACCOUNTS_BEZ_TS = 'accounts-bez-ts';
    public const TABLE_COLUMN_NAME_ROW_NUMBER = 'column-name-row-number';
    public const TABLE_MULTI_PK = 'multi-pk';
    public const TABLE_OUT_CSV_2COLS = 'out.csv_2Cols';
    public const TABLE_OUT_LEMMA = 'out.lemma';
    public const TABLE_OUT_NO_TIMESTAMP_TABLE = 'out.no_timestamp_table';
    public const TABLE_TABLE = 'table';
    public const TABLE_TYPES = 'types';
    public const TESTS_PREFIX = 'import-export-test_';

    /** @var Connection */
    protected $connection;

    /** @var SqlCommandBuilder */
    protected $qb;

    /** @var SQLServer2012Platform|AbstractPlatform */
    protected $platform;

    protected function setUp(): void
    {
        parent::setUp();
        $this->connection = $this->getExasolConnection();
    }

    protected function tearDown(): void
    {
        $this->connection->close();
        parent::tearDown();
    }

    private function getExasolConnection(): Connection
    {
        return ExasolConnection::getConnection(
            (string) getenv('EXASOL_HOST'),
            (string) getenv('EXASOL_USERNAME'),
            (string) getenv('EXASOL_PASSWORD')
        );
    }
}
