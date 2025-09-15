<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional\Handler;

use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateDevBranchCommand;
use Keboola\StorageDriver\Command\Project\CreateDevBranchResponse;
use Keboola\StorageDriver\Command\Project\DropDevBranchCommand;
use Keboola\StorageDriver\Snowflake\Features;
use Keboola\StorageDriver\Snowflake\Handler\Project\CreateDevBranchHandler;
use Keboola\StorageDriver\Snowflake\Handler\Project\DropDevBranchHandler;
use Keboola\StorageDriver\Snowflake\NameGenerator;
use Keboola\StorageDriver\Snowflake\Tests\Functional\BaseProjectTestCase;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

final class DevBranchHandlerTest extends BaseProjectTestCase
{
    public function testCreateDropDevBranch(): void
    {
        $command = new CreateDevBranchCommand([
            'stackPrefix' => $this->getTestPrefix(),
            'projectId' => '123',
            'branchId' => '456',
            'projectRoleName' => $this->projectResponse->getProjectRoleName(),
            'projectReadOnlyRoleName' => $this->projectResponse->getProjectReadOnlyRoleName(),
        ]);

        $roleName = (new NameGenerator($command->getStackPrefix()))
            ->createReadOnlyRoleNameForBranch(
                $command->getProjectId(),
                $command->getBranchId(),
            );
        $this->connection->executeQuery(sprintf(
            'DROP ROLE IF EXISTS %s;',
            SnowflakeQuote::quoteSingleIdentifier(
                $roleName,
            ),
        ));

        $response = (new CreateDevBranchHandler)(
            $this->getCurrentProjectCredentials(),
            $command,
            [
                Features::FEATURE_INPUT_MAPPING_READ_ONLY_STORAGE,
            ],
            new RuntimeOptions(),
        );
        $this->assertInstanceOf(CreateDevBranchResponse::class, $response);
        $this->assertSame($roleName, $response->getDevBranchReadOnlyRoleName());

        // assert that ro for branch was created
        $roles = $this->connection->fetchAllAssociative(sprintf(
            'SHOW ROLES STARTS WITH %s;',
            SnowflakeQuote::quote(
                $roleName,
            ),
        ));
        $this->assertCount(1, $roles);

        // assert that project ro role was granted to branch ro role
        $grants = $this->connection->fetchAllAssociative(sprintf(
            'SHOW GRANTS TO ROLE %s;',
            SnowflakeQuote::quoteSingleIdentifier(
                $roleName,
            ),
        ));
        $this->assertCount(1, $grants, var_export($grants, true));
        $this->assertSame('ROLE', $grants[0]['granted_to']);
        $this->assertSame('ROLE', $grants[0]['granted_on']);
        $this->assertSame('USAGE', $grants[0]['privilege']);
        $this->assertSame($roleName, $grants[0]['grantee_name']);
        $this->assertSame(
            $command->getProjectReadOnlyRoleName(),
            $grants[0]['name'],
        );

        // assert that branch ro role was granted to project role
        $grants = $this->connection->fetchAllAssociative(sprintf(
            'SHOW GRANTS TO ROLE %s;',
            SnowflakeQuote::quoteSingleIdentifier(
                $command->getProjectRoleName(),
            ),
        ));
        // expected count of grants in project role
        $this->assertCount(7, $grants, var_export($grants, true));
        // find grant for branch ro role
        $grants = array_filter($grants, function ($grant) use ($roleName) {
            // project role has ownership and usage on branch ro role
            return $grant['name'] === $roleName && $grant['privilege'] === 'USAGE';
        });
        // test usage grant
        $this->assertCount(1, $grants, var_export($grants, true));
        $grant = reset($grants);
        $this->assertSame('ROLE', $grant['granted_to']);
        $this->assertSame('ROLE', $grant['granted_on']);
        $this->assertSame('USAGE', $grant['privilege']);
        $this->assertSame($command->getProjectRoleName(), $grant['grantee_name']);
        $this->assertSame($roleName, $grant['name']);

        $response = (new DropDevBranchHandler)(
            $this->getCurrentProjectCredentials(),
            (new DropDevBranchCommand([
                'devBranchReadOnlyRoleName' => $roleName,
            ])),
            [],
            new RuntimeOptions(),
        );
        $this->assertNull($response);

        // assert that ro for branch no longer exists
        $roles = $this->connection->fetchAllAssociative(sprintf(
            'SHOW ROLES STARTS WITH %s;',
            SnowflakeQuote::quote(
                $roleName,
            ),
        ));
        $this->assertCount(0, $roles);
    }

    public function testCreateDevBranchWithoutReadOnlyStorage(): void
    {
        $command = new CreateDevBranchCommand([
            'stackPrefix' => $this->getTestPrefix(),
            'projectId' => '123',
            'branchId' => '456',
            'projectRoleName' => $this->projectResponse->getProjectRoleName(),
            'projectReadOnlyRoleName' => $this->projectResponse->getProjectReadOnlyRoleName(),
        ]);

        $roleName = (new NameGenerator($command->getStackPrefix()))
            ->createReadOnlyRoleNameForBranch(
                $command->getProjectId(),
                $command->getBranchId(),
            );
        $this->connection->executeQuery(sprintf(
            'DROP ROLE IF EXISTS %s;',
            SnowflakeQuote::quoteSingleIdentifier(
                $roleName,
            ),
        ));

        $response = (new CreateDevBranchHandler)(
            $this->getCurrentProjectCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );
        $this->assertNull($response);

        // assert that ro for branch was not created
        $roles = $this->connection->fetchAllAssociative(sprintf(
            'SHOW ROLES STARTS WITH %s;',
            SnowflakeQuote::quote(
                $roleName,
            ),
        ));
        $this->assertCount(0, $roles);

        // try call drop it should do nothing
        $response = (new DropDevBranchHandler)(
            $this->getCurrentProjectCredentials(),
            (new DropDevBranchCommand()),
            [],
            new RuntimeOptions(),
        );
        $this->assertNull($response);
    }
}
