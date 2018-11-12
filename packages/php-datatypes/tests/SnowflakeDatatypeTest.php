<?php
namespace Keboola\DataypeTest;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\Datatype\Definition\Snowflake;

class SnowflakeDatatypeTest extends \PHPUnit_Framework_TestCase
{
    public function testValid()
    {
        new Snowflake("VARCHAR", ["length" => "50"]);
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
        new Snowflake("NUMERIC", ["length" => ""]);
        new Snowflake("INTEGER", ["length" => ""]);
        new Snowflake("NUMERIC", ["length" => "38,0"]);
        new Snowflake("NUMERIC", ["length" => "38,38"]);
        new Snowflake("NUMERIC", ["length" => "38"]);
    }

    /**
     * @dataProvider invalidNumericLengths
     * @param $length
     */
    public function testInvalidNumericLengths($length)
    {
        try {
            new Snowflake("NUMERIC", ["length" => $length]);
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }

    public function testValidDateTimeLengths()
    {
        new Snowflake("datetime");
        new Snowflake("DATETIME");
        new Snowflake("DATETIME", ["length" => ""]);
        new Snowflake("TIMESTAMP", ["length" => ""]);
        new Snowflake("TIMESTAMP_LTZ", ["length" => "4"]);
        new Snowflake("TIMESTAMP_TZ", ["length" => "0"]);
        new Snowflake("TIMESTAMP_NTZ", ["length" => "9"]);
        new Snowflake("TIME", ["length" => "9"]);
    }

    public function testValidBinaryLengths()
    {
        new Snowflake("binary");
        new Snowflake("varbinary");
        new Snowflake("VARBINARY");
        new Snowflake("BINARY", ["length" => ""]);
        new Snowflake("VARBINARY", ["length" => ""]);
        new Snowflake("BINARY", ["length" => "1"]);
        new Snowflake("VARBINARY", ["length" => "1"]);
        new Snowflake("BINARY", ["length" => "8388608"]);
        new Snowflake("VARBINARY", ["length" => "8388608"]);
    }

    public function testSqlDefinition()
    {
        $definition = new Snowflake("NUMERIC", ["length" => ""]);
        $this->assertTrue($definition->getSQLDefinition() === "NUMERIC");

        $definition = new Snowflake("TIMESTAMP_TZ", ["length" => "0"]);
        $this->assertTrue($definition->getSQLDefinition() === "TIMESTAMP_TZ(0)");

        $definition = new Snowflake("TIMESTAMP_TZ", ["length" => "9"]);
        $this->assertTrue($definition->getSQLDefinition() === "TIMESTAMP_TZ(9)");

        $definition = new Snowflake("TIMESTAMP_TZ", ["length" => ""]);
        $this->assertTrue($definition->getSQLDefinition() === "TIMESTAMP_TZ");

        $definition = new Snowflake("TIMESTAMP_TZ");
        $this->assertTrue($definition->getSQLDefinition() === "TIMESTAMP_TZ");
    }

    /**
     * @dataProvider invalidDateTimeLengths
     * @param $length
     */
    public function testInvalidDateTimeLengths($length)
    {
        try {
            new Snowflake("DATETIME", ["length" => $length]);
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }

    public function testInvalidOption()
    {
        try {
            new Snowflake("NUMERIC", ["myoption" => "value"]);
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals(InvalidOptionException::class, get_class($e));
        }
    }

    public function testValidCharacterLengths()
    {
        new Snowflake("string");
        new Snowflake("STRING");
        new Snowflake("STRING", ["length" => ""]);
        new Snowflake("STRING", ["length" => "1"]);
        new Snowflake("STRING", ["length" => "16777216"]);
    }

    /**
     * @dataProvider invalidCharacterLengths
     * @param $length
     */
    public function testInvalidCharacterLengths($length)
    {
        try {
            new Snowflake("STRING", ["length" => $length]);
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }

    /**
     * @dataProvider invalidBinaryLengths
     * @param $length
     */
    public function testInvalidBinaryLengths($length)
    {
        try {
            new Snowflake("BINARY", ["length" => $length]);
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }

        try {
            new Snowflake("VARBINARY", ["length" => $length]);
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }

    public function testBasetypes()
    {
        foreach (Snowflake::TYPES as $type) {
            $basetype = (new Snowflake($type))->getBasetype();
            switch ($type) {
                case "INT":
                case "INTEGER":
                case "BIGINT":
                case "SMALLINT":
                case "TINYINT":
                case "BYTEINT":
                    $this->assertEquals("INTEGER", $basetype);
                    break;
                case "NUMBER":
                case "DECIMAL":
                case "NUMERIC":
                    $this->assertEquals("NUMERIC", $basetype);
                    break;
                case "FLOAT":
                case "FLOAT4":
                case "FLOAT8":
                case "DOUBLE":
                case "DOUBLE PRECISION":
                case "REAL":
                    $this->assertEquals("FLOAT", $basetype);
                    break;
                case "BOOLEAN":
                    $this->assertEquals("BOOLEAN", $basetype);
                    break;
                case "DATE":
                    $this->assertEquals("DATE", $basetype);
                    break;
                case "DATETIME":
                case "TIMESTAMP":
                case "TIMESTAMP_NTZ":
                case "TIMESTAMP_LTZ":
                case "TIMESTAMP_TZ":
                    $this->assertEquals("TIMESTAMP", $basetype);
                    break;
                default:
                    $this->assertEquals("STRING", $basetype);
                    break;
            }
        }
    }

    public function testVariant()
    {
        new Snowflake("VARIANT");
    }

    public function invalidNumericLengths()
    {
        return [
            ["notANumber"],
            ["0,0"],
            ["39,0"],
            ["-10,-5"],
            ["-5,-10"],
            ["38,a"],
            ["a,38"],
            ["a,a"]
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

    public function invalidDateTimeLengths()
    {
        return [
            ["notANumber"],
            ["0,0"],
            ["-1"],
            ["10"],
            ["a"],
            ["a,a"]
        ];
    }

    public function invalidBinaryLengths()
    {
        return [
            ["a"],
            ["0"],
            ["8388609"],
            ["-1"]
        ];
    }
}
