<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Exasol;

class ConnectionTest extends ExasolBaseCase
{
    public function test(): void
    {
        $this->testConnection();
    }
}
