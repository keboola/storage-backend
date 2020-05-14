<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Auth\Grant\Synapse;

use Keboola\TableBackendUtils\Auth\Grant\Synapse\GrantOptions;
use PHPUnit\Framework\TestCase;

class GrantOptionsTest extends TestCase
{
    public function testOptions(): void
    {
        $optionsDefault = new GrantOptions();
        $this->assertFalse($optionsDefault->isAllowGrantOption());

        $optionsDontAllowGrant = new GrantOptions(true);
        $this->assertTrue($optionsDontAllowGrant->isAllowGrantOption());
    }
}
