<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional;

use Doctrine\DBAL\Connection;
use Keboola\KeyGenerator\PemKeyCertificatePair;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateDevBranchCommand;
use Keboola\StorageDriver\Command\Project\CreateDevBranchResponse;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Snowflake\ConnectionFactory;
use Keboola\StorageDriver\Snowflake\Features;
use Keboola\StorageDriver\Snowflake\Handler\Project\CreateDevBranchHandler;
use Keboola\StorageDriver\Snowflake\NameGenerator;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use function Keboola\Utils\returnBytes;

abstract class BaseProjectTestCase extends BaseCase
{
    public const BASE_PROJECT_ID = '123';

    protected PemKeyCertificatePair $keyPair;

    protected CreateProjectResponse $projectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->dropProjectForTest($this->getTestPrefix(), self::BASE_PROJECT_ID);
        [$this->projectResponse, $this->keyPair] = $this->createProjectForTest(
            $this->getTestPrefix(),
            self::BASE_PROJECT_ID,
            [
                Features::FEATURE_INPUT_MAPPING_READ_ONLY_STORAGE,
            ],
        );
    }

    protected function tearDown(): void
    {
        var_export('Tearing down project'.PHP_EOL);
        parent::tearDown();
        $this->dropProjectForTest(
            $this->getTestPrefix(),
            self::BASE_PROJECT_ID,
        );
    }

    protected function getCurrentProjectCredentials(): GenericBackendCredentials
    {
        return $this->createProjectCredentials(
            $this->projectResponse,
            $this->keyPair,
        );
    }

    protected function getCurrentProjectConnection(): Connection
    {
        return ConnectionFactory::createFromCredentials($this->getCurrentProjectCredentials());
    }

    protected function createDevBranch(): CreateDevBranchResponse
    {
        $this->connection->executeQuery(sprintf(
            'DROP ROLE IF EXISTS %s',
            SnowflakeQuote::quoteSingleIdentifier(
                (new NameGenerator($this->getTestPrefix()))->createReadOnlyRoleNameForBranch(
                    '123',
                    '456',
                ),
            ),
        ));
        $response = (new CreateDevBranchHandler)(
            $this->getCurrentProjectCredentials(),
            new CreateDevBranchCommand([
                'stackPrefix' => $this->getTestPrefix(),
                'projectId' => '123',
                'branchId' => '456',
                'projectRoleName' => $this->projectResponse->getProjectRoleName(),
                'projectReadOnlyRoleName' => $this->projectResponse->getProjectReadOnlyRoleName(),
            ]),
            [
                Features::FEATURE_INPUT_MAPPING_READ_ONLY_STORAGE,
            ],
            new RuntimeOptions(),
        );
        assert($response !== null);
        return $response;
    }
}
