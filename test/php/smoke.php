<?php

declare(strict_types=1);

use MongoDB\Driver\BulkWrite;
use MongoDB\Driver\Command;
use MongoDB\Driver\Manager;
use MongoDB\Driver\Query;
use MongoDB\Driver\ReadPreference;
use MongoDB\Driver\ServerApi;
use MongoDB\Driver\WriteConcern;

$plainUri = 'mongodb://mongobetween:27016/?retryWrites=true';
$secureHost = getenv('SMOKE_SECURE_HOST') ?: 'mongobetween-secure';
$password = getenv('SMOKE_PROXY_PASSWORD');
$db = 'smoke_' . bin2hex(random_bytes(4));
$ns = $db . '.items';

$manager = new Manager($plainUri);
// w=3 (every member) rather than majority: a secondary read must see the data on whichever member it picks.
$majority = new WriteConcern(3, 10000);

function insertDocs(Manager $manager, string $ns, int $count, WriteConcern $wc, ?MongoDB\Driver\Session $session = null): void
{
    $bulk = new BulkWrite();
    for ($i = 0; $i < $count; $i++) {
        $bulk->insert(['n' => $i, 'tag' => 'x']);
    }
    $opts = $session !== null ? ['session' => $session] : [];
    $manager->executeBulkWrite($ns, $bulk, $opts + ['writeConcern' => $wc]);
}

function expect(bool $cond, string $msg): void
{
    if (!$cond) {
        throw new RuntimeException($msg);
    }
}

$checks = [
    'ping' => function () use ($manager) {
        $r = $manager->executeCommand('admin', new Command(['ping' => 1]))->toArray()[0];
        expect((float) $r->ok === 1.0, 'ok != 1');
    },
    'insertFind' => function () use ($manager, $ns, $majority) {
        insertDocs($manager, $ns, 10, $majority);
        $cursor = $manager->executeQuery($ns, new Query([], ['batchSize' => 2]));
        $n = count($cursor->toArray());
        expect($n === 10, "expected 10 docs, got $n");
    },
    'secondaryRead' => function () use ($manager, $ns) {
        $cursor = $manager->executeQuery($ns, new Query([]), ['readPreference' => new ReadPreference('secondary')]);
        $n = count($cursor->toArray());
        expect($n === 10, "expected 10 docs from secondary, got $n");
    },
    'dbAggregateSecondary' => function () use ($manager, $db) {
        $docs = [];
        for ($i = 0; $i < 7; $i++) {
            $docs[] = ['i' => $i];
        }
        $cmd = new Command([
            'aggregate' => 1,
            'pipeline' => [['$documents' => $docs]],
            'cursor' => ['batchSize' => 2],
        ]);
        $cursor = $manager->executeReadCommand($db, $cmd, ['readPreference' => new ReadPreference('secondary')]);
        $n = count($cursor->toArray());
        expect($n === 7, "expected 7 docs, got $n");
    },
    'transaction' => function () use ($manager, $db, $majority) {
        $txNs = $db . '.tx';
        // collection must exist before it is used inside a transaction
        $manager->executeCommand($db, new Command(['create' => 'tx']));
        $session = $manager->startSession();
        $session->startTransaction([]);
        $bulk = new BulkWrite();
        $bulk->insert(['_id' => 'tx1']);
        $manager->executeBulkWrite($txNs, $bulk, ['session' => $session]);
        $session->commitTransaction();
        $cursor = $manager->executeQuery($txNs, new Query(['_id' => 'tx1']));
        expect(count($cursor->toArray()) === 1, 'committed doc not visible');
    },
    'isMasterCommand' => function () use ($manager) {
        $r = $manager->executeCommand('admin', new Command(['isMaster' => 1]))->toArray()[0];
        expect(($r->msg ?? null) === 'isdbgrid', 'msg != isdbgrid');
        expect(($r->maxWireVersion ?? 0) >= 21, 'maxWireVersion < 21: ' . ($r->maxWireVersion ?? 'unset'));
    },
    'serverApiPing' => function () use ($plainUri) {
        $m = new Manager($plainUri, [], ['serverApi' => new ServerApi('1')]);
        $r = $m->executeCommand('admin', new Command(['ping' => 1]))->toArray()[0];
        expect((float) $r->ok === 1.0, 'ok != 1');
    },
    'earlyCursorDestroy' => function () use ($manager, $ns) {
        $cursor = $manager->executeQuery($ns, new Query([], ['batchSize' => 2]));
        foreach ($cursor as $doc) {
            break;
        }
        unset($cursor);
        // connection must still be usable afterwards
        $manager->executeCommand('admin', new Command(['ping' => 1]));
    },
    'authTls' => function () use ($secureHost, $password, $ns) {
        expect(is_string($password) && $password !== '', 'SMOKE_PROXY_PASSWORD not set');
        $base = "$secureHost:27017";
        $tls = 'tls=true&tlsCAFile=/certs/ca.pem';

        $anon = new Manager("mongodb://$base/?$tls");
        try {
            $anon->executeQuery($ns, new Query([]))->toArray();
            throw new RuntimeException('unauthenticated find unexpectedly succeeded');
        } catch (MongoDB\Driver\Exception\Exception $e) {
            expect($e->getCode() === 13, 'unauthenticated find: expected code 13, got ' . $e->getCode() . ' ' . $e->getMessage());
        }

        $auth = new Manager('mongodb://app:' . rawurlencode($password) . "@$base/?$tls");
        $r = $auth->executeCommand('admin', new Command(['ping' => 1]))->toArray()[0];
        expect((float) $r->ok === 1.0, 'authenticated ping ok != 1');
    },
];

$failed = false;
foreach ($checks as $name => $fn) {
    try {
        $fn();
        echo "PASS $name\n";
    } catch (Throwable $e) {
        $failed = true;
        echo "FAIL $name: " . get_class($e) . ': ' . $e->getMessage() . "\n";
    }
}

try {
    $manager->executeCommand($db, new Command(['dropDatabase' => 1]));
} catch (Throwable $e) {
    // best-effort cleanup
}

exit($failed ? 1 : 0);
