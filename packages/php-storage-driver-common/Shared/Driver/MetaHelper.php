<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\Driver;

use Google\Protobuf\Any;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;

final class MetaHelper
{
    /**
     * returns meta only if instance of expected class, otherwise throws exception
     * @param class-string $expectedMetaInstance
     */
    public static function getMetaRestricted(Message $command, string $expectedMetaInstance): ?Message
    {
        $meta = self::getMeta($command);
        if ($meta === null) {
            return null;
        }

        if (!$meta instanceof $expectedMetaInstance) {
            throw new Exception(sprintf(
                'Unexpected meta instance "%s" expected "%s"',
                get_class($meta),
                $expectedMetaInstance,
            ));
        }

        return $meta;
    }

    /**
     * returns meta instance if exists
     */
    public static function getMeta(Message $command): ?Message
    {
        if (!method_exists($command, 'getMeta')) {
            return null;
        }

        /** @var Any|null $meta */
        $meta = $command->getMeta();
        if ($meta === null) {
            return null;
        }

        return $meta->unpack();
    }
}
