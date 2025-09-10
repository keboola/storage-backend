<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Unit;

use Keboola\StorageDriver\Snowflake\NameGenerator;
use PHPUnit\Framework\TestCase;

class NameGeneratorTest extends TestCase
{
    public function testCreateReadOnlyRoleNameForBranch(): void
    {
        $nameGenerator = new NameGenerator('KBC_prefix_');
        $projectId = $nameGenerator->createReadOnlyRoleNameForBranch('234', '567');
        $this->assertSame('KBC_PREFIX_234_567_RO', $projectId);
    }
}
