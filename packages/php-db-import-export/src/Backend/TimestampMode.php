<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

enum TimestampMode
{
    case None;
    case CurrentTime;
    case FromSource;
}
