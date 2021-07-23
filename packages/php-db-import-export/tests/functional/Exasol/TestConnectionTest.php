<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Exasol;

use Tests\Keboola\Db\ImportExportFunctional\Exasol\ExasolBaseTestCase;

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
