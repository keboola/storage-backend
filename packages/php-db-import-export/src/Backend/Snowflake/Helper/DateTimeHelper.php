<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\Helper;

final class DateTimeHelper
{
    public const FORMAT = 'Y-m-d H:i:s';

    public static function getNowFormatted(): string
    {
        $currentDate = new \DateTime('now', new \DateTimeZone('UTC'));
        return $currentDate->format(self::FORMAT);
    }
}
