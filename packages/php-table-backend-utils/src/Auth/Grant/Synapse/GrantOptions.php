<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Auth\Grant\Synapse;

use Keboola\TableBackendUtils\Auth\Grant\GrantOptionsInterface;

final class GrantOptions implements GrantOptionsInterface
{
    public const OPTION_ALLOW_GRANT_OPTION = true;
    public const OPTION_DONT_ALLOW_GRANT_OPTION = false;

    /** @var bool|self::OPTION_ALLOW_GRANT_OPTION|self::OPTION_DONT_ALLOW_GRANT_OPTION */
    private $allowGrantOption = self::OPTION_DONT_ALLOW_GRANT_OPTION;

    /** @var array<Permission::GRANT_*> */
    private $permissions;

    /** @var string */
    private $grantTo;

    /** @var null|GrantOn::ON_* */
    private $subject;

    /** @var string[] */
    private $grantOnTargetPath = [];

    /**
     * @param array<Permission::GRANT_*> $permissions
     */
    public function __construct(array $permissions, string $grantTo)
    {
        $this->permissions = $permissions;
        $this->grantTo = $grantTo;
    }

    /**
     * @param self::OPTION_ALLOW_GRANT_OPTION|self::OPTION_DONT_ALLOW_GRANT_OPTION $isAllowed
     */
    public function setAllowGrantOption(bool $isAllowed): self
    {
        $this->allowGrantOption = $isAllowed;

        return $this;
    }

    /**
     * @param null|GrantOn::ON_* $subject
     */
    public function grantOnSubject(?string $subject): self
    {
        $this->subject = $subject;
        return $this;
    }

    public function isAllowGrantOption(): bool
    {
        return $this->allowGrantOption;
    }

    public function getPermissions(): array
    {
        return $this->permissions;
    }

    /**
     * @return null|GrantOn::ON_*
     */
    public function getSubject(): ?string
    {
        return $this->subject;
    }

    /**
     * @param string[] $grantOnTargetPath
     */
    public function setOnTargetPath(array $grantOnTargetPath): self
    {
        $this->grantOnTargetPath = $grantOnTargetPath;
        return $this;
    }

    /**
     * @return string[]
     */
    public function getOnTargetPath(): array
    {
        return $this->grantOnTargetPath;
    }

    public function getGrantTo(): string
    {
        return $this->grantTo;
    }
}
