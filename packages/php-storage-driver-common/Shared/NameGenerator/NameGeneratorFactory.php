<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\NameGenerator;

use Keboola\StorageDriver\Shared\BackendSupportsInterface;

final class NameGeneratorFactory
{
    public static function getGeneratorForBackendAndPrefix(
        string $backend,
        string $stackPrefix
    ): BackendNameGeneratorInterface {
        switch ($backend) {
            case BackendSupportsInterface::BACKEND_SYNAPSE:
                return new SynapseNameGenerator($stackPrefix);
        }
        return new GenericNameGenerator($stackPrefix);
    }
}
