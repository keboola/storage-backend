<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Exasol;

class TestConnectionTest extends ExasolBaseCase
{
    public function test(): void
    {
        $this->testConnection();
    }
}
