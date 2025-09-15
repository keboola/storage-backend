<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional;

use Doctrine\DBAL\Connection;
use Keboola\KeyGenerator\PemKeyCertificatePair;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Snowflake\ConnectionFactory;
use Keboola\StorageDriver\Snowflake\Features;

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
}
