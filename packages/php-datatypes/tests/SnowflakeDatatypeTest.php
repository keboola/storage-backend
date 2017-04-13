<?php
namespace Keboola\DataypeTest;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\Datatype\Definition\Snowflake;

class SnowflakeDatatypeTest extends \PHPUnit_Framework_TestCase
{
    public function testValid()
    {
        new Snowflake("VARCHAR", "50");
    }

    public function testInvalidType()
    {
        try {
            new Snowflake("UNKNOWN");
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals(InvalidTypeException::class, get_class($e));
        }
    }

    public function testValidNumericLengths()
    {
        new Snowflake("numeric");
        new Snowflake("NUMERIC");
        new Snowflake("NUMERIC", "");
        new Snowflake("INTEGER", "");
        new Snowflake("NUMERIC", "38,0");
    }

    /**
     * @dataProvider invalidNumericLengths
     * @param $length
     */
    public function testInvalidNumericLengths($length)
    {
        try {
            new Snowflake("NUMERIC", $length);
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }

    public function testValidCharacterLengths()
    {
        new Snowflake("string");
        new Snowflake("STRING");
        new Snowflake("STRING", "");
        new Snowflake("STRING", "1");
        new Snowflake("STRING", "16777216");
    }

    /**
     * @dataProvider invalidCharacterLengths
     * @param $length
     */
    public function testInvalidCharacterLengths($length)
    {
        try {
            new Snowflake("STRING", $length);
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }

    public function invalidNumericLengths()
    {
        return [
            ["notANumber"],
            ["10"],
            ["0,0"],
            ["39,0"],
            ["-10,-5"],
            ["-5,-10"],
            ["38,a"],
            ["a,38"],
            ["a,a"],
            ["10,10"],
            ["38,38"]
        ];
    }

    public function invalidCharacterLengths()
    {
        return [
            ["a"],
            ["0"],
            ["16777217"],
            ["-1"]
        ];
    }
}
