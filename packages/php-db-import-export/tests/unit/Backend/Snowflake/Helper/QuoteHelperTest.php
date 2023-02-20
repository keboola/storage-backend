<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\Helper;

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\Helper;

use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use PHPUnit\Framework\TestCase;

class QuoteHelperTest extends TestCase
{
    public function testQuote(): void
    {
        $result = QuoteHelper::quote('string');
        self::assertEquals('\'string\'', $result);
    }

    public function testQuoteIdentifier(): void
    {
        $result = QuoteHelper::quoteIdentifier('string');
        self::assertEquals('"string"', $result);
    }
}
