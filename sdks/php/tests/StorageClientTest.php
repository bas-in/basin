<?php

declare(strict_types=1);

namespace Basin\Tests;

use Basin\Http\Transport;
use Basin\Storage\StorageBucketClient;
use Basin\Storage\StorageClient;
use Basin\Types\Bucket;
use Basin\Types\DownloadResult;
use Basin\Types\SignedUrl;
use Basin\Types\StorageObject;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;

class StorageClientTest extends TestCase
{
    private function makeStorage(MockHandler $mock, ?string $projectId = 'proj_01J'): StorageClient
    {
        $stack = HandlerStack::create($mock);
        $transport = new Transport('http://localhost:8080', handler: $stack);
        return new StorageClient($transport, fn() => $projectId);
    }

    private function bucketBody(): string
    {
        return json_encode([
            'id' => 'bkt_01J', 'name' => 'avatars', 'public' => false,
            'file_size_limit' => null, 'allowed_mime_types' => [],
            'created_at' => '2024-01-01T00:00:00Z', 'updated_at' => '2024-01-01T00:00:00Z',
        ]);
    }

    private function objectBody(): string
    {
        return json_encode([
            'id' => 'obj_01J', 'bucket_id' => 'bkt_01J', 'path' => 'user.jpg',
            'size' => 12345, 'mime_type' => 'image/jpeg', 'metadata' => null,
            'owner' => 'usr_01J', 'etag' => '"abc123"',
            'created_at' => '2024-01-01T00:00:00Z', 'updated_at' => '2024-01-01T00:00:00Z',
        ]);
    }

    // ------------------------------------------------------------------
    // Bucket operations
    // ------------------------------------------------------------------

    public function testCreateBucket(): void
    {
        $mock = new MockHandler([
            new Response(201, ['Content-Type' => 'application/json'], $this->bucketBody()),
        ]);
        $storage = $this->makeStorage($mock);

        $bucket = $storage->createBucket('avatars', public: false);

        $this->assertInstanceOf(Bucket::class, $bucket);
        $this->assertSame('avatars', $bucket->name);
        $this->assertFalse($bucket->public);
    }

    public function testGetBucket(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], $this->bucketBody()),
        ]);
        $storage = $this->makeStorage($mock);

        $bucket = $storage->getBucket('avatars');

        $this->assertInstanceOf(Bucket::class, $bucket);
    }

    public function testDeleteBucket(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], '{"ok":true}'),
        ]);
        $storage = $this->makeStorage($mock);

        $result = $storage->deleteBucket('avatars');
        $this->assertTrue($result['ok']);
    }

    // ------------------------------------------------------------------
    // Object operations
    // ------------------------------------------------------------------

    public function testUpload(): void
    {
        $mock = new MockHandler([
            new Response(201, ['Content-Type' => 'application/json'], $this->objectBody()),
        ]);
        $storage = $this->makeStorage($mock);

        $obj = $storage->fromBucket('avatars')->upload('user.jpg', 'binary-data', 'image/jpeg');

        $this->assertInstanceOf(StorageObject::class, $obj);
        $this->assertSame('user.jpg', $obj->path);
        $this->assertSame('image/jpeg', $obj->mimeType);
    }

    public function testDownload(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'image/jpeg', 'ETag' => '"abc"'], 'binary-data'),
        ]);
        $storage = $this->makeStorage($mock);

        $result = $storage->fromBucket('avatars')->download('user.jpg');

        $this->assertInstanceOf(DownloadResult::class, $result);
        $this->assertSame('binary-data', $result->data);
        $this->assertSame('image/jpeg', $result->contentType);
        $this->assertSame('"abc"', $result->etag);
    }

    public function testRemove(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], '{"ok":true}'),
        ]);
        $storage = $this->makeStorage($mock);

        $result = $storage->fromBucket('avatars')->remove('user.jpg');
        $this->assertTrue($result['ok']);
    }

    public function testList(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'],
                json_encode([$this->objectBody() ? json_decode($this->objectBody(), true) : []])),
        ]);
        $storage = $this->makeStorage($mock);

        $objects = $storage->fromBucket('avatars')->list(prefix: 'user');

        $this->assertIsArray($objects);
        $this->assertInstanceOf(StorageObject::class, $objects[0]);
    }

    public function testRemoveByPrefixes(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], '{"ok":true}'),
        ]);
        $storage = $this->makeStorage($mock);

        $result = $storage->fromBucket('avatars')->removeByPrefixes(['folder/', 'temp/']);
        $this->assertTrue($result['ok']);
    }

    // ------------------------------------------------------------------
    // URLs
    // ------------------------------------------------------------------

    public function testGetPublicUrl(): void
    {
        $mock = new MockHandler([]);
        $storage = $this->makeStorage($mock, 'proj_01J');

        $url = $storage->fromBucket('avatars')->getPublicUrl('user/photo.jpg');

        $this->assertStringContainsString('/storage/v1/object/public/proj_01J/avatars/user/photo.jpg', $url);
    }

    public function testCreateSignedUrl(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], json_encode([
                'signedUrl'  => '/storage/v1/object/sign/proj_01J/avatars/user.jpg?token=abc',
                'expiresAt' => '2024-12-31T23:59:59Z',
            ])),
        ]);
        $storage = $this->makeStorage($mock);

        $signed = $storage->fromBucket('avatars')->createSignedUrl('user.jpg', expiresIn: 3600);

        $this->assertInstanceOf(SignedUrl::class, $signed);
        $this->assertStringContainsString('token=abc', $signed->absoluteUrl);
        $this->assertSame('2024-12-31T23:59:59Z', $signed->expiresAt);
    }

    public function testPathWithSpacesEncoded(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], $this->objectBody()),
        ]);

        $container = [];
        $stack = HandlerStack::create($mock);
        $stack->push(\GuzzleHttp\Middleware::history($container));
        $transport = new Transport('http://localhost:8080', handler: $stack);
        $storage = new StorageClient($transport, fn() => 'proj_01J');

        $storage->fromBucket('my bucket')->download('path/to/my file.jpg');

        $uri = (string) $container[0]['request']->getUri();
        $this->assertStringContainsString('my%20bucket', $uri);
        $this->assertStringContainsString('my%20file.jpg', $uri);
    }
}
