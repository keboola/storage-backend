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

        $optionsDontAllowGrant = new GrantOptions(GrantOptions::OPTION_ALLOW_GRANT_OPTION);
        $this->assertTrue($optionsDontAllowGrant->isAllowGrantOption());

        $optionsDontAllowGrant = new GrantOptions(GrantOptions::OPTION_DONT_ALLOW_GRANT_OPTION);
        $this->assertFalse($optionsDontAllowGrant->isAllowGrantOption());
    }
}
