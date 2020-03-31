<?php
namespace Keboola\DataypeTest;

use Keboola\Datatype\Definition\Exception\InvalidCompressionException;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\Datatype\Definition\Redshift;

class RedshiftDatatypeTest extends \PHPUnit_Framework_TestCase
{
    public function testValid()
    {
        new Redshift("VARCHAR", ["length" => "50"]);
    }

    public function testInvalidType()
    {
        try {
            new Redshift("UNKNOWN");
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals(InvalidTypeException::class, get_class($e));
        }
    }

    /**
     * @param string $columnType
     * @param array $lengthOption
     * @param string $expectedOutput
     * @dataProvider validLengthsProvider
     */
    public function testValidLengths($columnType, $option, $expectedOutput)
    {
        foreach ([$columnType, strtoupper($columnType)] as $item) {
            $redshift = new Redshift($item, $option);
            $this->assertEquals($expectedOutput, $redshift->getLength());
        }
    }

    /**
     * @param string $columnType
     * @param array $options
     * @dataProvider invalidLengthsProvider
     */
    public function testInvalidLengths($columnType, $options)
    {
        foreach ([$columnType, strtoupper($columnType)] as $item) {
            foreach ($options as $option) {
                try {
                    new Redshift($item, ['length' => $option]);
                } catch (\Exception $e) {
                    $this->assertEquals(InvalidLengthException::class, get_class($e));
                }
            }
        }
    }

    public function testValidCompressions()
    {
        new Redshift("VARCHAR", ["compression" => "RAW"]);
        new Redshift("VARCHAR", ["compression" => "raw"]);
        new Redshift("VARCHAR", ["compression" => "BYTEDICT"]);
        new Redshift("INT", ["compression" => "DELTA"]);
        new Redshift("INT", ["compression" => "DELTA32K"]);
        new Redshift("VARCHAR", ["compression" => "LZO"]);
        new Redshift("BIGINT", ["compression" => "MOSTLY8"]);
        new Redshift("BIGINT", ["compression" => "MOSTLY16"]);
        new Redshift("BIGINT", ["compression" => "MOSTLY32"]);
        new Redshift("VARCHAR", ["compression" => "RUNLENGTH"]);
        new Redshift("VARCHAR", ["compression" => "TEXT255"]);
        new Redshift("VARCHAR", ["compression" => "TEXT32K"]);
        new Redshift("VARCHAR", ["compression" => "ZSTD"]);
    }

    public function testInvalidOption()
    {
        try {
            new Redshift("NUMERIC", ["myoption" => "value"]);
            $this->fail("Exception not caught");
        } catch (\Exception $e) {
            $this->assertEquals(InvalidOptionException::class, get_class($e));
        }
    }

    public function testSQLDefinition()
    {
        $datatype = new Redshift("VARCHAR", ["length" => "50", "nullable" => true, "compression" => "ZSTD"]);
        $this->assertEquals("VARCHAR(50) ENCODE ZSTD", $datatype->getSQLDefinition());
    }

    public function testToArray()
    {
        $datatype = new Redshift("VARCHAR");
        $this->assertEquals(
            ["type" => "VARCHAR", "length" => null, "nullable" => true, "compression" => null],
            $datatype->toArray()
        );

        $datatype = new Redshift("VARCHAR", ["length" => "50", "nullable" => false, "compression" => "ZSTD"]);
        $this->assertEquals(
            ["type" => "VARCHAR", "length" => "50", "nullable" => false, "compression" => "ZSTD"],
            $datatype->toArray()
        );
    }

    public function testToMetadata()
    {
        $datatype = new Redshift("VARCHAR", ["length" => "50", "nullable" => false, "default" => "", "compression" => "ZSTD"]);

        $md = $datatype->toMetadata();
        $hasCompression = false;
        foreach ($md as $mdat) {
            $this->assertArrayHasKey("key", $mdat);
            if ($mdat["key"] === "KBC.datatype.compression") {
                $this->assertEquals("ZSTD", $mdat["value"]);
                $hasCompression = true;
            }
        }
        if (!$hasCompression) {
            $this->fail("Redshift datatype metadata should produce compression data if present");
        }

        $datatype = new Redshift("VARCHAR");
        $md = $datatype->toMetadata();
        foreach ($md as $mdat) {
            $this->assertArrayHasKey("key", $mdat);
            if ($mdat["key"] === "KBC.datatyp.compression") {
                $this->fail("Redshift datatype should not produce compression metadata if compression is not set");
            }
        }
    }

    public function testBasetypes()
    {
        foreach (Redshift::TYPES as $type) {
            $basetype = (new Redshift($type))->getBasetype();
            switch ($type) {
                case "SMALLINT":
                case "INT2":
                case "INTEGER":
                case "INT":
                case "INT4":
                case "BIGINT":
                case "INT8":
                    $this->assertEquals("INTEGER", $basetype);
                    break;
                case "DECIMAL":
                case "NUMERIC":
                    $this->assertEquals("NUMERIC", $basetype);
                    break;
                case "REAL":
                case "FLOAT4":
                case "DOUBLE PRECISION":
                case "FLOAT8":
                case "FLOAT":
                    $this->assertEquals("FLOAT", $basetype);
                    break;
                case "BOOLEAN":
                case "BOOL":
                    $this->assertEquals("BOOLEAN", $basetype);
                    break;
                case "DATE":
                    $this->assertEquals("DATE", $basetype);
                    break;
                case "TIMESTAMP":
                case "TIMESTAMP WITHOUT TIME ZONE":
                case "TIMESTAMPTZ":
                case "TIMESTAMP WITH TIME ZONE":
                    $this->assertEquals("TIMESTAMP", $basetype);
                    break;
                default:
                    $this->assertEquals("STRING", $basetype);
                    break;
            }
        }
    }

    public function invalidCompressions()
    {
        return [
            ["BOOLEAN", "BYTEDICT"],
            ["VARCHAR", "DELTA"],
            ["VARCHAR", "DELTA32K"],
            ["VARCHAR", "MOSTLY8"],
            ["VARCHAR", "MOSTLY16"],
            ["VARCHAR", "MOSTLY32"],
            ["NUMERIC", "TEXT255"],
            ["NUMERIC","TEXT32K"]
        ];
    }

    public function validLengthsProvider()
    {
        return [
            [
                'int',
                [],
                ''
            ],
            [
                'int',
                ['length' => ''],
                ''
            ],
            [
                'numeric',
                [],
                ''
            ],
            [
                'numeric',
                ['length' => ''],
                ''
            ],
            [
                'numeric',
                ['length' => ''],
                ''
            ],
            [
                'numeric',
                ['length' => '37,0'],
                '37,0'
            ],
            [
                'numeric',
                ['length' => '37,37'],
                '37,37'
            ],
            [
                'numeric',
                ['length' => '37'],
                '37'
            ],
            [
                'numeric',
                ['length' => ['numeric_precision' => '37']],
                '37'
            ],
            [
                'numeric',
                ['length' => ['numeric_precision' => '37', 'numeric_scale' => '37']],
                '37,37'
            ],
            [
                'numeric',
                ['length' => ['numeric_scale' => '37']],
                ''
            ],
            [
                'varchar',
                [],
                ''
            ],
            [
                'varchar',
                ['length' => ''],
                ''
            ],
            [
                'varchar',
                ['length' => '1'],
                '1'
            ],
            [
                'varchar',
                ['length' => '65535'],
                '65535'
            ],
            [
                'varchar',
                ['length' => ['character_maximum' => 65535]],
                '65535'
            ],
            [
                'varchar',
                ['length' => []],
                ''
            ],
            [
                'char',
                [],
                ''
            ],
            [
                'char',
                ['length' => '1'],
                '1'
            ],
            [
                'char',
                ['length' => '4096'],
                '4096'
            ],
            [
                'timestamptz',
                ['length' => null],
                ''
            ],
            [
                'timestamp',
                ['length' => null],
                ''
            ],
            [
                'timestamp',
                ['length' => ""],
                ''
            ],
            [
                'timestamp',
                ['length' => '8'],
                '8'
            ],
            [
                'timestamptz',
                ['length' => ''],
                ''
            ],
            [
                'timestamptz',
                ['length' => '8'],
                '8'
            ],
        ];
    }

    public function invalidLengthsProvider()
    {
        return [
            [
                'int',
                [
                    'notInt',
                    '1',
                    '255',
                    '256',
                    '-1'
                ]
            ],
            [
                'numeric',
                [
                    'notANumber',
                    '0,0',
                    '38,0',
                    '-10,-5',
                    '-5,-10',
                    '37,a',
                    'a,37',
                    'a,a',
                ],
            ],
            [
                'varchar',
                [
                    'a',
                    '0',
                    '65536',
                    '-1'
                ],
            ],
            [
                'char',
                [
                    'a',
                    '0',
                    '4097',
                    '-1',
                ],
            ],
            [
                'timestamp',
                [
                    '-1',
                    '15',
                    'abc',
                    '8,3',
                ],
            ],
            [
                'timestamptz',
                [
                    '-1',
                    '15',
                    'abc',
                    '8,3',
                ],
            ],
        ];
    }
}
