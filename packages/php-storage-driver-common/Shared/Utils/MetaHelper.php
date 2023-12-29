<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\Utils;

use Google\Protobuf\Any;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;

final class MetaHelper
{
    /**
     * @param class-string $expectedMetaInstance
     */
    public static function getMetaFromCommand(Message $command, string $expectedMetaInstance): ?Message
    {
        if (!method_exists($command, 'getMeta')) {
            return null;
        }

        /** @var Any|null $meta */
        $meta = $command->getMeta();
        if ($meta === null) {
            return null;
        }

        $meta = $meta->unpack();
        if (!$meta instanceof $expectedMetaInstance) {
            throw new Exception(sprintf(
                'Unexpected meta instance "%s" expected "%s"',
                get_class($meta),
                $expectedMetaInstance,
            ));
        }

        return $meta;
    }
}
