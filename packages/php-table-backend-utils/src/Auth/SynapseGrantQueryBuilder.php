<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Auth;

use Keboola\TableBackendUtils\Auth\Grant\GrantOptionsInterface;
use Keboola\TableBackendUtils\Auth\Grant\RevokeOptionsInterface;
use Keboola\TableBackendUtils\Auth\Grant\Synapse\GrantOn;
use Keboola\TableBackendUtils\Auth\Grant\Synapse\GrantOptions;
use Keboola\TableBackendUtils\Auth\Grant\Synapse\RevokeOptions;
use Keboola\TableBackendUtils\Escaping\SynapseQuote;

class SynapseGrantQueryBuilder implements GrantQueryBuilderInterface
{

    /**
     * @param GrantOptions $options
     */
    public function getGrantSql(GrantOptionsInterface $options): string
    {
        $on = $this->getOnStatement($options->getSubject(), $options->getOnTargetPath());

        $with = '';
        if ($options->isAllowGrantOption() === true) {
            $with = ' WITH GRANT OPTION';
        }

        return sprintf(
            'GRANT %s%s TO %s%s',
            implode(', ', $options->getPermissions()),
            $on,
            SynapseQuote::quoteSingleIdentifier($options->getGrantTo()),
            $with
        );
    }

    /**
     * @param RevokeOptions $options
     */
    public function getRevokeSql(RevokeOptionsInterface $options): string
    {
        $on = $this->getOnStatement($options->getSubject(), $options->getOnTargetPath());

        $with = '';
        if ($options->isRevokedInCascade() === true) {
            $with = ' CASCADE';
        }

        $permissions = implode(', ', $options->getPermissions());

        if ($options->isGrantOptionRevoked() === true) {
            $permissions = 'GRANT OPTION FOR '.$permissions;
        }

        return sprintf(
            'REVOKE %s%s FROM %s%s',
            $permissions,
            $on,
            SynapseQuote::quoteSingleIdentifier($options->getRevokeFrom()),
            $with
        );
    }

    /**
     * @param string[] $onTargetPath
     * @return string[]
     */
    private function getQuotedTargetPath(array $onTargetPath): array
    {
        return array_map(fn(string $pathPart) => SynapseQuote::quoteSingleIdentifier($pathPart), $onTargetPath);
    }

    /**
     * @param null|GrantOn::ON_* $grantSubject
     * @param string[] $grantOnTargetPath
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
