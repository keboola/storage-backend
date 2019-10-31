<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\Helper;

final class DateTimeHelper
{
    public static function getNowFormatted(): string
    {
        $currentDate = new \DateTime('now', new \DateTimeZone('UTC'));
        return $currentDate->format('Y-m-d H:i:s');
    }
}
