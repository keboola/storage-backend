<?php

declare(strict_types=1);

namespace Keboola\DatatypeTest;

use Keboola\Datatype\Definition\Common;
use PHPUnit\Framework\TestCase;

class CommonDatatypeTest extends TestCase
{
    public function testConstructor(): void
    {
        $datatype = $this->getMockForAbstractClass(Common::class, ['VARCHAR']);
        $this->assertEquals('VARCHAR', $datatype->getType());
        $this->assertNull($datatype->getLength());
        $this->assertNull($datatype->getDefault());
        $this->assertTrue($datatype->isNullable());

        $datatype = $this->getMockForAbstractClass(
            Common::class,
            ['VARCHAR', ['length' => '50', 'nullable' => false, 'default' => '']]
        );
        $this->assertTrue($datatype->getLength() === '50');
        $this->assertTrue(!$datatype->isNullable());
        $this->assertTrue($datatype->getDefault() === '');

        $datatype = $this->getMockForAbstractClass(
            Common::class,
            ['VARCHAR', ['length' => 50, 'nullable' => false, 'default' => 123]]
        );
        $this->assertTrue($datatype->getLength() === '50');
        $this->assertTrue(!$datatype->isNullable());
        $this->assertTrue($datatype->getDefault() === '123');
    }

    public function testToMetadata(): void
    {
        $datatype = $this->getMockForAbstractClass(Common::class, ['VARCHAR']);
        $datatype->method('getBasetype')->willReturn('STRING');
        $md = $datatype->toMetadata();
        foreach ($md as $mdat) {
            $this->assertArrayHasKey('key', $mdat);
            $this->assertArrayHasKey('value', $mdat);
            if ($mdat['key'] === Common::KBC_METADATA_KEY_TYPE) {
                $this->assertEquals('VARCHAR', $mdat['value']);
            } elseif ($mdat['key'] === Common::KBC_METADATA_KEY_LENGTH) {
                $this->fail('unspecified length should not create metadata.');
            } elseif ($mdat['key'] === Common::KBC_METADATA_KEY_NULLABLE) {
                $this->assertEquals(true, $mdat['value']);
            } elseif ($mdat['key'] === Common::KBC_METADATA_KEY_FORMAT) {
                $this->fail('unspecified format should not create metadata.');
            } elseif ($mdat['key'] === Common::KBC_METADATA_KEY_DEFAULT) {
                $this->assertEquals('NULL', $mdat['value']);
            } elseif ($mdat['key'] === Common::KBC_METADATA_KEY_BASETYPE) {
                $this->assertEquals('STRING', $mdat['value']);
            }
        }

        $datatype = $this->getMockForAbstractClass(Common::class, ['NUMERIC', [
            'length' => '10,0',
            'nullable' => false,
            'default' => '0',
        ]]);
        $datatype->method('getBasetype')->willReturn('NUMERIC');
        $md = $datatype->toMetadata();
        foreach ($md as $mdat) {
            $this->assertArrayHasKey('key', $mdat);
            $this->assertArrayHasKey('value', $mdat);
            if ($mdat['key'] === Common::KBC_METADATA_KEY_TYPE) {
                $this->assertEquals('NUMERIC', $mdat['value']);
            } elseif ($mdat['key'] === Common::KBC_METADATA_KEY_LENGTH) {
                $this->assertEquals('10,0', $mdat['value']);
            } elseif ($mdat['key'] === Common::KBC_METADATA_KEY_NULLABLE) {
                $this->assertEquals(false, $mdat['value']);
            } elseif ($mdat['key'] === Common::KBC_METADATA_KEY_DEFAULT) {
                $this->assertEquals('0', $mdat['value']);
            } elseif ($mdat['key'] === Common::KBC_METADATA_KEY_BASETYPE) {
                $this->assertEquals('NUMERIC', $mdat['value']);
            }
        }
    }
}
