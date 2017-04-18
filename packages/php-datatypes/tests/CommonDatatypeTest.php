<?php
namespace Keboola\DataypeTest;

use Keboola\Datatype\Definition\Common;

class CommonDatatypeTest extends \PHPUnit_Framework_TestCase
{
    public function testConstructor()
    {
        $datatype = new Common("VARCHAR");
        $this->assertEquals("VARCHAR", $datatype->getType());
        $this->assertEquals("", $datatype->getLength());
        $this->assertEquals(true, $datatype->isNullable());

        $datatype = new Common("VARCHAR", "50", false);
        $this->assertEquals("50", $datatype->getLength());
        $this->assertEquals(false, $datatype->isNullable());
    }

    public function testSQLDefinition()
    {
        $datatype = new Common("VARCHAR");
        $this->assertEquals("VARCHAR", $datatype->getSQLDefinition());

        $datatype = new Common("VARCHAR", "50", true);
        $this->assertEquals("VARCHAR(50)", $datatype->getSQLDefinition());
    }

    public function testToArray()
    {
        $datatype = new Common("VARCHAR");
        $this->assertEquals(["type" => "VARCHAR", "length" => "", "nullable" => true], $datatype->toArray());

        $datatype = new Common("VARCHAR", "50", true);
        $this->assertEquals(["type" => "VARCHAR", "length" => "50", "nullable" => true], $datatype->toArray());
    }
}
