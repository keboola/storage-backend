<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\Shared\Driver;

use Google\Protobuf\Any;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Backend\InitBackendResponse;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand\CreateBucketTeradataMeta;
use Keboola\StorageDriver\Shared\Driver\MetaHelper;
use PHPUnit\Framework\TestCase;
use Throwable;

class MetaHelperTest extends TestCase
{
    public function testGetMetaNotContainMeta(): void
    {
        $result = MetaHelper::getMeta(
            $this->createMock(Message::class),
        );
        $this->assertNull($result);
    }

    public function testGetMetaNoMetaSet(): void
    {
        $result = MetaHelper::getMeta(
            new InitBackendResponse(),
        );
        $this->assertNull($result);
    }

    public function testGetMetaCorrectMetaInstance(): void
    {
        $meta = new Any();
        $meta->pack(
            (new InitBackendResponse\InitBackendSynapseMeta())
                ->setGlobalRoleName('test'),
        );

        $result = MetaHelper::getMeta(
            (new InitBackendResponse())->setMeta($meta),
        );
        $this->assertInstanceOf(InitBackendResponse\InitBackendSynapseMeta::class, $result);
        $this->assertSame('test', $result->getGlobalRoleName());
    }

    public function testGetMetaRestrictedNotContainMeta(): void
    {
        $result = MetaHelper::getMetaRestricted(
            $this->createMock(Message::class),
            InitBackendResponse\InitBackendSynapseMeta::class,
        );
        $this->assertNull($result);
    }

    public function testGetMetaRestrictedNoMetaSet(): void
    {
        $result = MetaHelper::getMetaRestricted(
            new InitBackendResponse(),
            InitBackendResponse\InitBackendSynapseMeta::class,
        );
        $this->assertNull($result);
    }

    public function testGetMetaRestrictedInvalidMetaInstance(): void
    {
        $meta = new Any();
        $meta->pack(
            new InitBackendResponse\InitBackendSynapseMeta(),
        );

        $this->expectException(Throwable::class);
        MetaHelper::getMetaRestricted(
            (new InitBackendResponse())->setMeta($meta),
            CreateBucketTeradataMeta::class,
        );
    }

    public function testGetMetaRestrictedCorrectMetaInstance(): void
    {
        $meta = new Any();
        $meta->pack(
            (new InitBackendResponse\InitBackendSynapseMeta())
                ->setGlobalRoleName('test'),
        );

        $result = MetaHelper::getMetaRestricted(
            (new InitBackendResponse())->setMeta($meta),
            InitBackendResponse\InitBackendSynapseMeta::class,
        );
        $this->assertInstanceOf(InitBackendResponse\InitBackendSynapseMeta::class, $result);
        $this->assertSame('test', $result->getGlobalRoleName());
    }
}
