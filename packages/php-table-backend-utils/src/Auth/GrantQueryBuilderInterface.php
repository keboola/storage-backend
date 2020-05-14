<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Auth;

use Keboola\TableBackendUtils\Auth\Grant\GrantOptionsInterface;
use Keboola\TableBackendUtils\Auth\Grant\RevokeOptionsInterface;

interface GrantQueryBuilderInterface
{
    /**
     * @param string[] $permissions
     * @param string[] $grantOnTargetPath
     */
    public function getGrantSql(
        array $permissions,
        ?string $grantSubject,
        array $grantOnTargetPath,
        string $to,
        ?GrantOptionsInterface $options
    ): string;

    /**
     * @param string[] $permissions
     * @param string[] $grantOnTargetPath
     */
    public function getRevokeSql(
        array $permissions,
        ?string $grantSubject,
        array $grantOnTargetPath,
        string $to,
        ?RevokeOptionsInterface $options
    ): string;
}
