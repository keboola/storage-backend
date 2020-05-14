<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Auth;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Keboola\TableBackendUtils\Auth\Grant\GrantOptionsInterface;
use Keboola\TableBackendUtils\Auth\Grant\RevokeOptionsInterface;
use Keboola\TableBackendUtils\Auth\Grant\Synapse\GrantOn;
use Keboola\TableBackendUtils\Auth\Grant\Synapse\GrantOptions;
use Keboola\TableBackendUtils\Auth\Grant\Synapse\Permission;
use Keboola\TableBackendUtils\Auth\Grant\Synapse\RevokeOptions;

class SynapseGrantQueryBuilder implements GrantQueryBuilderInterface
{
    /** @var SQLServer2012Platform|AbstractPlatform */
    private $platform;

    public function __construct(Connection $connection)
    {
        $this->platform = $connection->getDatabasePlatform();
    }

    /**
     * @param array<Permission::GRANT_*> $permissions
     * @param null|GrantOn::ON_* $grantSubject
     * @param string[] $grantOnTargetPath
     * @param GrantOptions|null $options
     */
    public function getGrantSql(
        array $permissions,
        ?string $grantSubject,
        array $grantOnTargetPath,
        string $to,
        ?GrantOptionsInterface $options
    ): string {
        $on = $this->getOnStatement($grantSubject, $grantOnTargetPath);

        $with = '';
        if ($options !== null && $options->isAllowGrantOption() === true) {
            $with = ' WITH GRANT OPTION';
        }

        return sprintf(
            'GRANT %s%s TO %s%s',
            implode(', ', $permissions),
            $on,
            $this->platform->quoteSingleIdentifier($to),
            $with
        );
    }

    /**
     * @param string[] $onTargetPath
     * @return string[]
     */
    private function getQuotedTargetPath(array $onTargetPath): array
    {
        return array_map(function (string $pathPart) {
            return $this->platform->quoteSingleIdentifier($pathPart);
        }, $onTargetPath);
    }

    /**
     * @param array<Permission::GRANT_*> $permissions
     * @param null|GrantOn::ON_* $grantSubject
     * @param string[] $grantOnTargetPath
     * @param RevokeOptions|null $options
     */
    public function getRevokeSql(
        array $permissions,
        ?string $grantSubject,
        array $grantOnTargetPath,
        string $to,
        ?RevokeOptionsInterface $options
    ): string {
        $on = $this->getOnStatement($grantSubject, $grantOnTargetPath);

        $with = '';
        if ($options !== null && $options->isCascade() === true) {
            $with = ' CASCADE';
        }

        $permissions = implode(', ', $permissions);

        if ($options !== null && $options->isAllowGrantOption() === true) {
            $permissions = 'GRANT OPTION FOR '.$permissions;
        }

        return sprintf(
            'REVOKE %s%s FROM %s%s',
            $permissions,
            $on,
            $this->platform->quoteSingleIdentifier($to),
            $with
        );
    }

    /**
     * @param null|GrantOn::ON_* $grantSubject
     * @param string[] $grantOnTargetPath
     * @return string
     */
    private function getOnStatement(?string $grantSubject, array $grantOnTargetPath): string
    {
        $on = '';
        if ($grantSubject !== null) {
            $path = '';
            if (count($grantOnTargetPath) !== 0) {
                $path = '::' . implode('.', $this->getQuotedTargetPath($grantOnTargetPath));
            }

            $on = sprintf(
                ' ON %s%s',
                $grantSubject,
                $path
            );
        }
        return $on;
    }
}
