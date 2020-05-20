<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Auth\Grant;

interface RevokeOptionsInterface
{
    /**
     * @return string[]
     */
    public function getPermissions(): array;

    public function getSubject(): ?string;

    /**
     * @return string[]
     */
    public function getOnTargetPath(): array;

    public function getRevokeFrom(): string;
}
