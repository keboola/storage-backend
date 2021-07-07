<?php

namespace Keboola\TableBackendUtils\Connection\Teradata;

use Doctrine\DBAL\Connection;
use Keboola\Connection\Storage\Service\Backend\ConnectionManager\BaseConnectionManager;
use Keboola\Connection\Storage\Service\Backend\ConnectionManager\ConnectionOptionsInterface;
use Keboola\Connection\Storage\Workspace\Configuration\TableWorkspaceConfiguration;
use Keboola\Exception as BucketsException;
use Model_Row_Bucket as Bucket;
use Model_Row_ConnectionMysql as BackendConnection;
use Model_Row_Project as Project;
use Throwable;

class TeradataConnection extends BaseConnectionManager
{
    public function getDefaultConnectionForBucket(Bucket $bucket, ?ConnectionOptionsInterface $options = null)
    {
        // TODO: Implement getDefaultConnectionForBucket() method.
    }

    public function getDefaultConnection(BackendConnection $connection, ?ConnectionOptionsInterface $options = null)
    {
        // TODO: Implement getDefaultConnection() method.
    }

    public function getDefaultConnectionForProject(Project $project, ?ConnectionOptionsInterface $options = null)
    {
        // TODO: Implement getDefaultConnectionForProject() method.
    }

    public function closeConnections(): void
    {
        // TODO: Implement closeConnections() method.
    }

    public function getBucketConnection(Bucket $bucket)
    {
        // TODO: Implement getBucketConnection() method.
    }

    public function getConnectionForWorkspaceInProject(
        TableWorkspaceConfiguration $workspace,
        string $password,
        Project $project
    ) {
        // TODO: Implement getConnectionForWorkspaceInProject() method.
    }

    public static function getConnectionForParameters($params): Connection
    {
        $params = array_merge($params, [
            'port' => 1025,
            'driverClass' => TeradataDriver::class,
        ]);

        try {
            return \Doctrine\DBAL\DriverManager::getConnection($params);
        } catch (Throwable $e) {
            throw new BucketsException($e->getMessage(), (int) $e->getCode(), $e);
        }
    }
}
