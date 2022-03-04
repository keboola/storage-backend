<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\Shared\Utils;

use Keboola\StorageDriver\Shared\Utils\Password;
use PHPUnit\Framework\TestCase;

class PasswordTest extends TestCase
{
    public function testGenerateDefaultPassword(): void
    {
        // run 1000 times
        for ($k = 0; $k < 1000; $k++) {
            $password = Password::generate();
            $this->assertPasswordHasNumber($password);
            $this->assertPasswordHasUppercase($password);
            $this->assertPasswordHasLowercase($password);
            $this->assertEquals(32, strlen($password));
            $this->assertMatchesRegularExpression(
                '~^[^0O1Il]+$~',
                $password,
                'Characters "0O1Il" are not excluded from password.'
            );
        }
    }

    private function assertPasswordHasNumber(string $password): void
    {
        $this->assertMatchesRegularExpression(
            '/[0-9]+/',
            $password,
            sprintf('Password "%s" missing numeric characters.', $password)
        );
    }

    private function assertPasswordHasUppercase(string $password): void
    {
        $this->assertMatchesRegularExpression(
            '/[A-Z]+/',
            $password,
            sprintf('Password "%s" missing uppercase characters.', $password)
        );
    }

    private function assertPasswordHasLowercase(string $password): void
    {
        $this->assertMatchesRegularExpression(
            '/[a-z]+/',
            $password,
            sprintf('Password "%s" missing lowercase characters.', $password)
        );
    }

    public function testGeneratePasswordWithSpecialCharacters(): void
    {
        // run 1000 times
        for ($k = 0; $k < 1000; $k++) {
            $password = Password::generate(
                50,
                Password::SET_LOWERCASE |
                Password::SET_UPPERCASE |
                Password::SET_NUMBER |
                Password::SET_SPECIAL_CHARACTERS
            );
            $this->assertPasswordHasNumber($password);
            $this->assertPasswordHasUppercase($password);
            $this->assertPasswordHasLowercase($password);
            $this->assertPasswordHasSpecialCharacter($password);
            $this->assertEquals(50, strlen($password));
            $this->assertMatchesRegularExpression(
                '~^[^0O1Il]+$~',
                $password,
                'Characters "0O1Il" are not excluded from password.'
            );
        }
    }

    private function assertPasswordHasSpecialCharacter(string $password): void
    {
        $this->assertMatchesRegularExpression(
            '/[_\-\!\$\.\+\/\@\#\%\&\*\?]+/',
            $password,
            sprintf('Password "%s" missing special characters.', $password)
        );
    }

    public function testGeneratePasswordExcludeCharacters(): void
    {
        // run 1000 times
        for ($k = 0; $k < 1000; $k++) {
            $password = Password::generate(
                50,
                Password::SET_LOWERCASE |
                Password::SET_UPPERCASE |
                Password::SET_NUMBER |
                Password::SET_SPECIAL_CHARACTERS,
                'abc'
            );
            $this->assertPasswordHasNumber($password);
            $this->assertPasswordHasUppercase($password);
            $this->assertPasswordHasLowercase($password);
            $this->assertPasswordHasSpecialCharacter($password);
            $this->assertEquals(50, strlen($password));
            $this->assertMatchesRegularExpression(
                '~^[^abc]+$~',
                $password,
                'Characters "abc" are not excluded from password.'
            );
        }
    }
}
