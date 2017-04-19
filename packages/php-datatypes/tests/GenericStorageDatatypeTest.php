<?php

namespace Keboola\DataypeTest;

use Keboola\Datatype\Definition\GenericStorage;

class GenericStorageDatatypeTest extends \PHPUnit_Framework_TestCase
{
    public function testGetBasetype()
    {
        $this->assertEquals("DATE", (new GenericStorage("date"))->getBasetype());
        $this->assertEquals("DATE", (new GenericStorage("DATE"))->getBasetype());
        $this->assertEquals("TIMESTAMP", (new GenericStorage("DateTime"))->getBasetype());
        $this->assertEquals("TIMESTAMP", (new GenericStorage("timestamp"))->getBasetype());
        $this->assertEquals("STRING", (new GenericStorage("dattim"))->getBasetype());

        $this->assertEquals("INTEGER", (new GenericStorage("int16"))->getBasetype());
        $this->assertEquals("INTEGER", (new GenericStorage("int"))->getBasetype());
        $this->assertEquals("INTEGER", (new GenericStorage("INTEGER"))->getBasetype());
        $this->assertEquals("FLOAT", (new GenericStorage("float8"))->getBasetype());
        $this->assertEquals("FLOAT", (new GenericStorage("REAL"))->getBasetype());
        $this->assertEquals("FLOAT", (new GenericStorage("double percision"))->getBasetype());
        $this->assertEquals("NUMERIC", (new GenericStorage("number"))->getBasetype());
        $this->assertEquals("NUMERIC", (new GenericStorage("DECIMAL"))->getBasetype());
        $this->assertEquals("NUMERIC", (new GenericStorage("numeric"))->getBasetype());

        $this->assertEquals("BOOLEAN", (new GenericStorage("BOOL"))->getBasetype());
        $this->assertEquals("BOOLEAN", (new GenericStorage("boolean"))->getBasetype());

        $this->assertEquals("STRING", (new GenericStorage("anythingelse"))->getBasetype());
    }

    public function testToMetadata()
    {
        $datatype = new GenericStorage("DATE", [
            'length' => 10,
            'nullable' => false,
            'default' => '1970-01-01',
            'format' => 'Y-m-d'
        ]);
        $datatypeMetadata = $datatype->toMetadata();

        foreach ($datatypeMetadata as $md) {
            $this->assertArrayHasKey('key', $md);
            $this->assertArrayHasKey('value', $md);
            if ($md['key'] === 'KBC.datatype.format') {
                $this->assertEquals('Y-m-d', $md['value']);
            }
            if ($md['key'] === 'KBC.datatype.default') {
                $this->assertEquals('1970-01-01', $md['value']);
            }
            if ($md['key'] === 'KBC.datatype.type') {
                $this->assertEquals('DATE', $md['value']);
            }
            if ($md['key'] === 'KBC.datatype.nullable') {
                $this->assertEquals(false, $md['value']);
            }
            if ($md['key'] === 'KBC.datatype.basetype') {
                $this->assertEquals('DATE', $md['value']);
            }
        }
    }

    public function testToArray()
    {
        $datatype = new GenericStorage("DATE", [
            'length' => 10,
            'nullable' => false,
            'default' => '1970-01-01',
            'format' => 'Y-m-d'
        ]);

        $this->assertEquals([
            'type' => 'DATE',
            'length' => '10',
            'nullable' => false,
            'default' => '1970-01-01',
            'format' => 'Y-m-d'], $datatype->toArray());
    }

    public function testSqlDefinition()
    {
        $datatype = new GenericStorage("DATE", [
            'length' => 10,
            'nullable' => false,
            'default' => '1970-01-01',
            'format' => 'Y-m-d'
        ]);

        $this->assertEquals("DATE(10) NOT NULL DEFAULT '1970-01-01'", $datatype->getSQLDefinition());

        $datatype = new GenericStorage("INTEGER", [
            'length' => 10
        ]);
        $this->assertEquals("INTEGER(10) NULL DEFAULT NULL", $datatype->getSQLDefinition());
    }
}
