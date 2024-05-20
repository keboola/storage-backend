<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\Helper;

use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;

final class DateTimeHelper
{
    public const FORMAT = 'Y-m-d H:i:s';

    public static function getNowFormatted(): string
    {
        $currentDate = new DateTimeImmutable('now', new DateTimeZone('UTC'));
        return self::getTimestampFormated($currentDate);
    }

    public static function getTimestampFormated(DateTimeInterface $timestamp): string
    {
        return $timestamp->format(self::FORMAT);
    }
}
