<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Exasol;

class TestConnectionTest extends ExasolBaseTestCase
{
    public function test(): void
    {
        self::assertEquals(
            1,
            $this->connection->fetchOne('SELECT 1')
        );
    }
}
