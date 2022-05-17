<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Auth\Grant\Synapse;

use Keboola\TableBackendUtils\Auth\Grant\RevokeOptionsInterface;

final class RevokeOptions implements RevokeOptionsInterface
{
    public const OPTION_REVOKE_GRANT_OPTION = true;
    public const OPTION_DONT_REVOKE_GRANT_OPTION = false;
    public const OPTION_REVOKE_CASCADE = true;
    public const OPTION_DONT_REVOKE_CASCADE = false;

    private bool $revokeGrantOption = self::OPTION_DONT_REVOKE_GRANT_OPTION;

    private bool $isCascade = self::OPTION_DONT_REVOKE_CASCADE;

    /** @var array<Permission::GRANT_*> */
    private array $permissions;

    private string $revokeFrom;

    /** @var string[] */
    private array $revokeOnTargetPath = [];

    /** @var null|GrantOn::ON_* */
    private $subject;

    /**
     * @param array<Permission::GRANT_*> $permissions
     */
    public function __construct(array $permissions, string $revokeFrom)
    {
        $this->permissions = $permissions;
        $this->revokeFrom = $revokeFrom;
    }

    public function isGrantOptionRevoked(): bool
    {
        return $this->revokeGrantOption;
    }

    public function isRevokedInCascade(): bool
    {
        return $this->isCascade;
    }

    /**
     * @param self::OPTION_REVOKE_CASCADE|self::OPTION_DONT_REVOKE_CASCADE $isCascade
     */
    public function revokeInCascade(bool $isCascade): self
    {
        $this->isCascade = $isCascade;
        return $this;
    }

    /**
     * @param self::OPTION_REVOKE_GRANT_OPTION|self::OPTION_DONT_REVOKE_GRANT_OPTION $revoke
     *
     * parameter $revokeGrantOption is kept here and on first place
     * because missing this doesn't make much sense, and could be expected
     * that removing grant option will appear in future version of synapse
     */
    public function revokeGrantOption(bool $revoke): self
    {
        if ($revoke === self::OPTION_REVOKE_GRANT_OPTION) {
            throw new \Exception('Revoking grant option is not supported on Synapse.');
        }

        $this->revokeGrantOption = $revoke;

        return $this;
    }

    /**
     * @param null|GrantOn::ON_* $subject
     */
    public function revokeOnSubject(?string $subject): self
    {
        $this->subject = $subject;
        return $this;
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
     * @param string[] $revokeOnTargetPath
     */
    public function setOnTargetPath(array $revokeOnTargetPath): self
    {
        $this->revokeOnTargetPath = $revokeOnTargetPath;
        return $this;
    }

    /**
     * @return string[]
     */
    public function getOnTargetPath(): array
    {
        return $this->revokeOnTargetPath;
    }

    public function getRevokeFrom(): string
    {
        return $this->revokeFrom;
    }
}
