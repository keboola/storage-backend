<?php
namespace Keboola\DataypeTest;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\Datatype\Definition\MySQL;

class MySQLDatatypeTest extends \PHPUnit_Framework_TestCase
{
    public function testValid()
    {
        new MySQL("VARCHAR", ["length" => "50"]);
    }

    public function testInvalidType()
    {
        try {
            new MySQL("UNKNOWN");
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals(InvalidTypeException::class, get_class($e));
        }
    }

    public function testInvalidOption()
    {
        try {
            new MySQL("NUMERIC", ["myoption" => "value"]);
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals(InvalidOptionException::class, get_class($e));
        }
    }

    public function testValidNumericLengths()
    {
        new MySQL("numeric");
        new MySQL("NUMERIC");
        new MySQL("NUMERIC", ["length" => ""]);
        new MySQL("NUMERIC", ["length" => "65,0"]);
        new MySQL("NUMERIC", ["length" => "65"]);
        new MySQL("NUMERIC", ["length" => "10,10"]);
        new MySQL('NUMERIC', [
            'length' => [
                'numeric_precision' => '38',
                'numeric_scale' => '0'
            ]
        ]);
        new MySQL('NUMERIC', [
            'length' => [
                'numeric_precision' => '20',
                'numeric_scale' => '20'
            ]
        ]);
        new MySQL('NUMERIC', [
            'length' => [
                'numeric_precision' => '20'
            ]
        ]);
        new MySQL('NUMERIC', [
            'length' => [
                'numeric_scale' => '20'
            ]
        ]);
    }

    /**
     * @dataProvider invalidNumericLengths
     * @param $length
     */
    public function testInvalidNumericLengths($length)
    {
        try {
            new MySQL("NUMERIC", ["length" => $length]);
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }

    public function testValidIntegerLengths()
    {
        new MySQL("integer");
        new MySQL("INTEGER");
        new MySQL("INTEGER", ["length" => ""]);
        new MySQL("INTEGER", ["length" => "255"]);
    }

    /**
     * @dataProvider invalidIntegerLengths
     * @param $length
     */
    public function testInvalidIntegerLengths($length)
    {
        try {
            new MySQL("INTEGER", ["length" => $length]);
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }


    public function testValidFloatLengths()
    {
        new MySQL("float");
        new MySQL("FLOAT");
        new MySQL("FLOAT", ["length" => ""]);
        new MySQL("FLOAT", ["length" => "255"]);
        new MySQL("FLOAT", ["length" => "255,0"]);
    }

    /**
     * @dataProvider invalidFloatLengths
     * @param $length
     */
    public function testInvalidFloatLengths($length)
    {
        try {
            new MySQL("FLOAT", ["length" => $length]);
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }


    public function testValidVariableCharacterLengths()
    {
        new MySQL("varchar", ["length" => "1"]);
        new MySQL("VARCHAR", ["length" => "1"]);
        new MySQL("VARCHAR", ["length" => "4294967295"]);
        new MySQL('VARCHAR', [
            'length' => [
                'character_maximum' => '16777216'
            ]
        ]);
    }

    public function testValidFixedCharacterLengths()
    {
        new MySQL("char");
        new MySQL("CHAR");
        new MySQL("CHAR", ["length" => ""]);
        new MySQL("CHAR", ["length" => "1"]);
        new MySQL("CHAR", ["length" => "255"]);
    }

    public function testVariableCharacterWithoutLength()
    {
        try {
            new MySQL("VARCHAR");
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }

    /**
     * @dataProvider invalidVariableCharacterLengths
     * @param $length
     */
    public function testInvalidVariableCharacterLengths($length)
    {
        try {
            new MySQL("VARCHAR", ["length" => $length]);
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }

    /**
     * @dataProvider invalidFixedCharacterLengths
     * @param $length
     */
    public function testInvalidFixedCharacterLengths($length)
    {
        try {
            new MySQL("CHAR", ["length" => $length]);
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }

    public function testBasetypes()
    {
        foreach (MySQL::TYPES as $type) {
            if ($type == 'VARCHAR') {
                $basetype = (new MySQL($type, ["length" => 255]))->getBasetype();
            } else {
                $basetype = (new MySQL($type))->getBasetype();
            }
            switch ($type) {
                case "INT":
                case "INTEGER":
                case "BIGINT":
                case "SMALLINT":
                case "TINYINT":
                case "MEDIUMINT":
                    $this->assertEquals("INTEGER", $basetype);
                    break;
                case "NUMERIC":
                case "DECIMAL":
                case "DEC":
                case "FIXED":
                    $this->assertEquals("NUMERIC", $basetype);
                    break;
                case "FLOAT":
                case "DOUBLE PRECISION":
                case "REAL":
                case "DOUBLE":
                    $this->assertEquals("FLOAT", $basetype);
                    break;
                case "DATE":
                    $this->assertEquals("DATE", $basetype);
                    break;
                case "DATETIME":
                case "TIMESTAMP":
                    $this->assertEquals("TIMESTAMP", $basetype);
                    break;
                default:
                    $this->assertEquals("STRING", $basetype);
                    break;
            }
        }
    }

    public function invalidNumericLengths()
    {
        return [
            ["notANumber"],
            ["0,0"],
            ["66,0"],
            ["-10,-5"],
            ["-5,-10"],
            ["66,a"],
            ["a,66"],
            ["a,a"],
            ["66,66"]
        ];
    }
    
    public function invalidIntegerLengths()
    {
        return [
            ["notANumber"],
            ["-1"],
            ["256"]
        ];
    }

    public function invalidFixedCharacterLengths()
    {
        return [
            ["a"],
            ["0"],
            ["256"],
            ["-1"]
        ];
    }


    public function invalidVariableCharacterLengths()
    {
        return [
            [""],
            ["a"],
            ["0"],
            ["4294967296"],
            ["-1"]
        ];
    }

    public function invalidVariableIntegerLengths()
    {
        return [
            ["-1"],
            ["256"],
            ["a"]
        ];
    }

    public function invalidFloatLengths()
    {
        return [
            ["notANumber"],
            ["0,0"],
            ["256,0"],
            ["-10,-5"],
            ["-5,-10"],
            ["256,a"],
            ["a,256"],
            ["a,a"],
            ["10,10"],
            ["256,256"]
        ];
    }
}
