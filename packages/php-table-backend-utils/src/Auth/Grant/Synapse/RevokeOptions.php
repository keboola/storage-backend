<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Auth\Grant\Synapse;

use Keboola\TableBackendUtils\Auth\Grant\RevokeOptionsInterface;

final class RevokeOptions implements RevokeOptionsInterface
{
    /** @var bool */
    private $allowGrantOption;

    /** @var bool */
    private $isCascade;

    /**
     * parameter $allowGrantOption is kept here and on first place
     * because missing this doesn't make much sense, and could be expected
     * that removing grant option will appear in future version of synapse
     */
    public function __construct(bool $allowGrantOption = false, bool $isCascade = false)
    {
        if ($allowGrantOption === true) {
            throw new \Exception('Revoking grant option is not supported on Synapse.');
        }
        $this->allowGrantOption = $allowGrantOption;
        $this->isCascade = $isCascade;
    }

    public function isAllowGrantOption(): bool
    {
        return $this->allowGrantOption;
    }

    public function isCascade(): bool
    {
        return $this->isCascade;
    }
}
