<?php

declare(strict_types=1);

namespace Keboola\DatatypeTest;

use DateTime;
use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\GenericStorage;
use PHPUnit\Framework\TestCase;

class GenericStorageDatatypeTest extends TestCase
{
    public function testGetBasetype(): void
    {
        $this->assertEquals('DATE', (new GenericStorage('date'))->getBasetype());
        $this->assertEquals('DATE', (new GenericStorage('DATE'))->getBasetype());
        $this->assertEquals('TIMESTAMP', (new GenericStorage(DateTime::class))->getBasetype());
        $this->assertEquals('TIMESTAMP', (new GenericStorage('timestamp'))->getBasetype());
        $this->assertEquals('STRING', (new GenericStorage('dattim'))->getBasetype());

        $this->assertEquals('INTEGER', (new GenericStorage('int8'))->getBasetype());
        $this->assertEquals('INTEGER', (new GenericStorage('int'))->getBasetype());
        $this->assertEquals('INTEGER', (new GenericStorage('INTEGER'))->getBasetype());
        $this->assertEquals('FLOAT', (new GenericStorage('float8'))->getBasetype());
        $this->assertEquals('FLOAT', (new GenericStorage('REAL'))->getBasetype());
        $this->assertEquals('FLOAT', (new GenericStorage('double precision'))->getBasetype());
        $this->assertEquals('NUMERIC', (new GenericStorage('number'))->getBasetype());
        $this->assertEquals('NUMERIC', (new GenericStorage('DECIMAL'))->getBasetype());
        $this->assertEquals('NUMERIC', (new GenericStorage('numeric'))->getBasetype());

        $this->assertEquals('BOOLEAN', (new GenericStorage('BOOL'))->getBasetype());
        $this->assertEquals('BOOLEAN', (new GenericStorage('boolean'))->getBasetype());

        $this->assertEquals('STRING', (new GenericStorage('enum'))->getBasetype());

        $this->assertEquals('STRING', (new GenericStorage('anythingelse'))->getBasetype());
    }

    public function testToMetadata(): void
    {
        $datatype = new GenericStorage('DATE', [
            'length' => 10,
            'nullable' => false,
            'default' => '1970-01-01',
            'format' => 'Y-m-d',
        ]);
        $datatypeMetadata = $datatype->toMetadata();

        foreach ($datatypeMetadata as $md) {
            $this->assertArrayHasKey('key', $md);
            $this->assertArrayHasKey('value', $md);
            if ($md['key'] === Common::KBC_METADATA_KEY_FORMAT) {
                $this->assertEquals('Y-m-d', $md['value']);
            }
            if ($md['key'] === Common::KBC_METADATA_KEY_DEFAULT) {
                $this->assertEquals('1970-01-01', $md['value']);
            }
            if ($md['key'] === Common::KBC_METADATA_KEY_TYPE) {
                $this->assertEquals('DATE', $md['value']);
            }
            if ($md['key'] === Common::KBC_METADATA_KEY_NULLABLE) {
                $this->assertEquals(false, $md['value']);
            }
            if ($md['key'] === Common::KBC_METADATA_KEY_BASETYPE) {
                $this->assertEquals('DATE', $md['value']);
            }
        }
        $datatype = new GenericStorage('VARCHAR');
        foreach ($datatype->toMetadata() as $md) {
            if ($md['key'] === Common::KBC_METADATA_KEY_FORMAT) {
                $this->fail('if format not specified, should not be included in metadata');
            }
        }
    }

    public function testToArray(): void
    {
        $datatype = new GenericStorage('DATE', [
            'length' => 10,
            'nullable' => false,
            'default' => '1970-01-01',
            'format' => 'Y-m-d',
        ]);

        $this->assertEquals([
            'type' => 'DATE',
            'length' => '10',
            'nullable' => false,
            'default' => '1970-01-01',
            'format' => 'Y-m-d'], $datatype->toArray());
    }

    public function testSqlDefinition(): void
    {
        $datatype = new GenericStorage('DATE', [
            'length' => 10,
            'nullable' => false,
            'default' => '1970-01-01',
            'format' => 'Y-m-d',
        ]);

        $this->assertEquals("DATE(10) NOT NULL DEFAULT '1970-01-01'", $datatype->getSQLDefinition());

        $datatype = new GenericStorage('INTEGER', [
            'length' => 10,
        ]);
        $this->assertEquals('INTEGER(10) NULL DEFAULT NULL', $datatype->getSQLDefinition());

        $datatype = new GenericStorage('VARCHAR', ['length' => '50', 'nullable' => false, 'default' => 'NULL']);
        $this->assertEquals("VARCHAR(50) NOT NULL DEFAULT 'NULL'", $datatype->getSQLDefinition());
    }

    public function testFalseyDefaults(): void
    {
        $datatype = new GenericStorage('INTEGER', [
            'length' => 11,
            'nullable' => false,
            'default' => 0,
        ]);

        $this->assertEquals("INTEGER(11) NOT NULL DEFAULT '0'", $datatype->getSQLDefinition());

        $hasDefaultMetadata = false;
        foreach ($datatype->toMetadata() as $metadatum) {
            if ($metadatum['key'] === Common::KBC_METADATA_KEY_DEFAULT && $metadatum['value'] === '0') {
                $hasDefaultMetadata = true;
            }
        }
        if (!$hasDefaultMetadata) {
            $this->fail('Should have default set to zero and output it to metadata');
        }

        $datatype = new GenericStorage('VARCHAR', ['length' => '50', 'nullable' => false, 'default' => '']);
        $this->assertEquals("VARCHAR(50) NOT NULL DEFAULT ''", $datatype->getSQLDefinition());

        $hasDefaultMetadata = false;
        foreach ($datatype->toMetadata() as $metadatum) {
            if ($metadatum['key'] === Common::KBC_METADATA_KEY_DEFAULT && $metadatum['value'] === '') {
                $hasDefaultMetadata = true;
            }
        }
        if (!$hasDefaultMetadata) {
            $this->fail("Should have default set to '' and output it to metadata");
        }
    }
}
