<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Auth\Grant\Synapse;

use Keboola\TableBackendUtils\Auth\Grant\Synapse\GrantOn;
use Keboola\TableBackendUtils\Auth\Grant\Synapse\Permission;
use Keboola\TableBackendUtils\Auth\Grant\Synapse\RevokeOptions;
use PHPUnit\Framework\TestCase;

class RevokeOptionsTest extends TestCase
{
    public function testOptionsDefault(): void
    {
        $options = new RevokeOptions([Permission::GRANT_VIEW_DEFINITION], 'ToMyUser');
        $this->assertSame(
            [Permission::GRANT_VIEW_DEFINITION],
            $options->getPermissions()
        );
        $this->assertFalse($options->isGrantOptionRevoked());
        $this->assertFalse($options->isRevokedInCascade());
        $this->assertSame([], $options->getOnTargetPath());
        $this->assertEquals('ToMyUser', $options->getRevokeFrom());
        $this->assertNull($options->getSubject());
    }

    public function testOptionsFluentInterface(): void
    {
        $options = (new RevokeOptions([Permission::GRANT_VIEW_DEFINITION], 'ToMyUser'))
            ->revokeOnSubject(GrantOn::ON_OBJECT)
            ->setOnTargetPath(['path'])
            ->revokeInCascade(RevokeOptions::OPTION_REVOKE_CASCADE)
            ->revokeGrantOption(RevokeOptions::OPTION_DONT_REVOKE_GRANT_OPTION);
        $this->assertSame(
            [Permission::GRANT_VIEW_DEFINITION],
            $options->getPermissions()
        );
        $this->assertTrue($options->isRevokedInCascade());
        $this->assertFalse($options->isGrantOptionRevoked());
        $this->assertSame(['path'], $options->getOnTargetPath());
        $this->assertEquals('ToMyUser', $options->getRevokeFrom());
        $this->assertEquals(GrantOn::ON_OBJECT, $options->getSubject());
    }

    public function testFailAllowGrantOption(): void
    {
        $this->expectException(\Throwable::class);
        $this->expectExceptionMessage('Revoking grant option is not supported on Synapse.');
        (new RevokeOptions([Permission::GRANT_VIEW_DEFINITION], 'ToMyUser'))
            ->revokeGrantOption(RevokeOptions::OPTION_REVOKE_GRANT_OPTION);
    }
}
