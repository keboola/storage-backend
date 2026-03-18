<?php

declare(strict_types=1);

namespace Keboola\DatatypeTest;

use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\DefinitionInterface;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class CommonDatatypeTest extends TestCase
{
    /**
     * @param array{length?:string|null, nullable?:bool, default?:string|null} $options
     */
    private function createCommonInstance(
        string $type,
        array $options = [],
        string $basetype = '',
    ): Common {
        return new class ($type, $options, $basetype) extends Common {
            private string $basetypeValue;

            /**
             * @param array{length?:string|null, nullable?:bool, default?:string|null} $options
             */
            public function __construct(string $type, array $options, string $basetype)
            {
                parent::__construct($type, $options);
                $this->basetypeValue = $basetype;
            }

            public function getBasetype(): string
            {
                return $this->basetypeValue;
            }

            /** @return array<mixed> */
            public function toArray(): array
            {
                return [];
            }

            public function getSQLDefinition(): string
            {
                return '';
            }

            public static function getTypeByBasetype(string $basetype): string
            {
                return '';
            }

            public static function getDefinitionForBasetype(string $basetype): DefinitionInterface
            {
                throw new RuntimeException('Not implemented');
            }
        };
    }

    public function testConstructor(): void
    {
        $datatype = $this->createCommonInstance('VARCHAR');
        $this->assertEquals('VARCHAR', $datatype->getType());
        $this->assertNull($datatype->getLength());
        $this->assertNull($datatype->getDefault());
        $this->assertTrue($datatype->isNullable());

        $datatype = $this->createCommonInstance(
            'VARCHAR',
            ['length' => '50', 'nullable' => false, 'default' => ''],
        );
        $this->assertTrue($datatype->getLength() === '50');
        $this->assertTrue(!$datatype->isNullable());
        $this->assertTrue($datatype->getDefault() === '');

        $datatype = $this->createCommonInstance(
            'VARCHAR',
            ['length' => '50', 'nullable' => false, 'default' => '123'],
        );
        $this->assertTrue($datatype->getLength() === '50');
        $this->assertTrue(!$datatype->isNullable());
        $this->assertTrue($datatype->getDefault() === '123');
    }

    public function testToMetadata(): void
    {
        $datatype = $this->createCommonInstance('VARCHAR', [], 'STRING');
        $md = $datatype->toMetadata();
        foreach ($md as $mdat) {
            $this->assertArrayHasKey('key', $mdat); // @phpstan-ignore method.alreadyNarrowedType
            $this->assertArrayHasKey('value', $mdat); // @phpstan-ignore method.alreadyNarrowedType
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

        $datatype = $this->createCommonInstance(
            'NUMERIC',
            [
                'length' => '10,0',
                'nullable' => false,
                'default' => '0',
            ],
            'NUMERIC',
        );
        $md = $datatype->toMetadata();
        foreach ($md as $mdat) {
            $this->assertArrayHasKey('key', $mdat); // @phpstan-ignore method.alreadyNarrowedType
            $this->assertArrayHasKey('value', $mdat); // @phpstan-ignore method.alreadyNarrowedType
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

    public function testToMetadataWithEmptyLength(): void
    {
        $datatype = $this->createCommonInstance(
            'NUMERIC',
            [
                'length' => '0',
                'nullable' => false,
                'default' => '0',
            ],
            'NUMERIC',
        );
        $md = $datatype->toMetadata();
        $this->assertSame(
            [
            [
                'key' => 'KBC.datatype.type',
                'value' => 'NUMERIC',
            ],
            [
                'key' => 'KBC.datatype.nullable',
                'value' => false,
            ],
            [
                'key' => 'KBC.datatype.basetype',
                'value' => 'NUMERIC',
            ],
            [
                'key' => 'KBC.datatype.length',
                'value' => '0',
            ],
            [
                'key' => 'KBC.datatype.default',
                'value' => '0',
            ],
            ],
            $md,
        );
    }
}
