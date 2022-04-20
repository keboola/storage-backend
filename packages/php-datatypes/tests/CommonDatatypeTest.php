<?php
namespace Keboola\DataypeTest;

use Keboola\Datatype\Definition\Common;
use PHPUnit\Framework\TestCase;

class CommonDatatypeTest extends TestCase
{
    public function testConstructor()
    {
        $datatype = $this->getMockForAbstractClass(Common::class, ["VARCHAR"]);
        $this->assertEquals("VARCHAR", $datatype->getType());
        $this->assertNull($datatype->getLength());
        $this->assertNull($datatype->getDefault());
        $this->assertTrue(true === $datatype->isNullable());

        $datatype = $this->getMockForAbstractClass(Common::class, ["VARCHAR", ["length" => "50", "nullable" => false, "default" => ""]]);
        $this->assertTrue("50" === $datatype->getLength());
        $this->assertTrue(false === $datatype->isNullable());
        $this->assertTrue("" === $datatype->getDefault());

        $datatype = $this->getMockForAbstractClass(Common::class, ["VARCHAR", ["length" => 50, "nullable" => false, "default" => 123]]);
        $this->assertTrue("50" === $datatype->getLength());
        $this->assertTrue(false === $datatype->isNullable());
        $this->assertTrue("123" === $datatype->getDefault());
    }

    public function testToMetadata()
    {
        $datatype = $this->getMockForAbstractClass(Common::class, ["VARCHAR"]);
        $datatype->method("getBasetype")->willReturn("STRING");
        $md = $datatype->toMetadata();
        foreach ($md as $mdat) {
            $this->assertArrayHasKey("key", $mdat);
            $this->assertArrayHasKey("value", $mdat);
            if ($mdat["key"] === Common::KBC_METADATA_KEY_TYPE) {
                $this->assertEquals("VARCHAR", $mdat["value"]);
            } else if ($mdat["key"] === Common::KBC_METADATA_KEY_LENGTH) {
                $this->fail("unspecified length should not create metadata.");
            } else if ($mdat["key"] === Common::KBC_METADATA_KEY_NULLABLE) {
                $this->assertEquals(true, $mdat["value"]);
            } else if ($mdat["key"] === Common::KBC_METADATA_KEY_FORMAT) {
                $this->fail("unspecified format should not create metadata.");
            } else if ($mdat["key"] === Common::KBC_METADATA_KEY_DEFAULT) {
                $this->assertEquals("NULL", $mdat["value"]);
            } else if ($mdat["key"] === Common::KBC_METADATA_KEY_BASETYPE) {
                $this->assertEquals("STRING", $mdat["value"]);
            }
        }

        $datatype = $this->getMockForAbstractClass(Common::class, ["NUMERIC", [
            "length" => "10,0",
            "nullable" => false,
            "default" => "0"
        ]]);
        $datatype->method("getBasetype")->willReturn("NUMERIC");
        $md = $datatype->toMetadata();
        foreach ($md as $mdat) {
            $this->assertArrayHasKey("key", $mdat);
            $this->assertArrayHasKey("value", $mdat);
            if ($mdat["key"] === Common::KBC_METADATA_KEY_TYPE) {
                $this->assertEquals("NUMERIC", $mdat["value"]);
            } else if ($mdat["key"] === Common::KBC_METADATA_KEY_LENGTH) {
                $this->assertEquals("10,0", $mdat["value"]);
            } else if ($mdat["key"] === Common::KBC_METADATA_KEY_NULLABLE) {
                $this->assertEquals(false, $mdat["value"]);
            } else if ($mdat["key"] === Common::KBC_METADATA_KEY_DEFAULT) {
                $this->assertEquals("0", $mdat["value"]);
            } else if ($mdat["key"] === Common::KBC_METADATA_KEY_BASETYPE) {
                $this->assertEquals("NUMERIC", $mdat["value"]);
            }
        }
    }
}
