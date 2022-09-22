<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Auth\Grant\Synapse;

use Keboola\TableBackendUtils\Auth\Grant\Synapse\GrantOn;
use Keboola\TableBackendUtils\Auth\Grant\Synapse\GrantOptions;
use Keboola\TableBackendUtils\Auth\Grant\Synapse\Permission;
use PHPUnit\Framework\TestCase;

class GrantOptionsTest extends TestCase
{
    public function testOptionsDefault(): void
    {
        $options = new GrantOptions([Permission::GRANT_VIEW_DEFINITION], 'ToMyUser');
        $this->assertSame(
            [Permission::GRANT_VIEW_DEFINITION],
            $options->getPermissions()
        );
        $this->assertFalse($options->isAllowGrantOption());
        $this->assertSame([], $options->getOnTargetPath());
        $this->assertEquals('ToMyUser', $options->getGrantTo());
        $this->assertNull($options->getSubject());
    }

    public function testOptionsFluentInterface(): void
    {
        $options = (new GrantOptions([Permission::GRANT_VIEW_DEFINITION], 'ToMyUser'))
            ->grantOnSubject(GrantOn::ON_OBJECT)
            ->setOnTargetPath(['path'])
            ->setAllowGrantOption(GrantOptions::OPTION_ALLOW_GRANT_OPTION);
        $this->assertSame(
            [Permission::GRANT_VIEW_DEFINITION],
            $options->getPermissions()
        );
        $this->assertTrue($options->isAllowGrantOption());
        $this->assertSame(['path'], $options->getOnTargetPath());
        $this->assertEquals('ToMyUser', $options->getGrantTo());
        $this->assertEquals(GrantOn::ON_OBJECT, $options->getSubject());
    }
}
