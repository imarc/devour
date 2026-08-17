<?php

use PHPUnit\Framework\TestCase;

class TestSynchronizer extends Devour\Synchronizer
{
	protected function assertMigrationReady(PDO $database): void
	{
	}
}

final class SynchronizerTest extends TestCase
{
	private function statsDatabase(): PDO
	{
		$database = new PDO('sqlite::memory:');
		$database->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
		$database->exec('CREATE TABLE devour_stats (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			start_time TEXT, scheduled_by TEXT, scheduled_time TEXT, end_time TEXT,
			canceled_time TEXT, canceled_by TEXT, heartbeat TEXT, max_gap INTEGER,
			tables TEXT, ids TEXT, force INTEGER, log TEXT
		)');
		$database->exec('CREATE TABLE devour_updates (target VARCHAR(255) PRIMARY KEY, time TIMESTAMP)');

		return $database;
	}


	public function testLoggingAppendsWithoutResendingTheWholeLog()
	{
		$database = $this->statsDatabase();

		$sync = new class($database, $database) extends TestSynchronizer {
			public function emit(string $message): void
			{
				$this->log($message);
			}
		};

		$sync->schedule([], [], 'admin@example.com');
		$sync->emit('first');
		$sync->emit('second');

		$log = $database->query('SELECT log FROM devour_stats LIMIT 1')->fetchColumn();

		$this->assertStringContainsString('first', $log);
		$this->assertStringContainsString('second', $log);
		$this->assertSame(1, substr_count($log, 'first'));
	}


	public function testLoggingRecordsAHeartbeat()
	{
		$database = $this->statsDatabase();

		$sync = new class($database, $database) extends TestSynchronizer {
			public function emit(string $message): void
			{
				$this->log($message);
			}
		};

		$sync->schedule([], [], 'admin@example.com');
		$sync->emit('working');

		$heartbeat = $database->query('SELECT heartbeat FROM devour_stats LIMIT 1')->fetchColumn();

		$this->assertNotNull($heartbeat);
		$this->assertNotFalse(strtotime($heartbeat));
	}


	public function testMaxGapRecordsTheLargestObservedInterval()
	{
		$database = $this->statsDatabase();

		$sync = new class($database, $database) extends TestSynchronizer {
			public ?int $fixedNow = NULL;

			public function emit(string $message): void
			{
				$this->log($message);
			}

			protected function now(): int
			{
				return $this->fixedNow ?? time();
			}
		};

		$sync->fixedNow = strtotime('2026-08-17 09:00:00');
		$sync->schedule([], [], 'admin@example.com');

		// first line only sets the heartbeat — the interval before it is not a gap between lines
		$sync->emit('one');

		$sync->fixedNow = strtotime('2026-08-17 09:00:10');   // 10s gap
		$sync->emit('two');

		$sync->fixedNow = strtotime('2026-08-17 09:01:40');   // 90s gap — the largest
		$sync->emit('three');

		$sync->fixedNow = strtotime('2026-08-17 09:01:45');   // 5s gap, must not lower the maximum
		$sync->emit('four');

		$this->assertSame(
			90,
			(int) $database->query('SELECT max_gap FROM devour_stats LIMIT 1')->fetchColumn()
		);
	}


	public function testScheduleStoresForceAndTables(): void
	{
		$database = new PDO('sqlite::memory:');
		$database->exec('CREATE TABLE devour_stats (id INTEGER PRIMARY KEY AUTOINCREMENT, start_time TEXT, scheduled_by TEXT, scheduled_time TEXT, end_time TEXT, canceled_time TEXT, canceled_by TEXT, heartbeat TEXT, max_gap INTEGER, tables TEXT, ids TEXT, force INTEGER, log TEXT)');
		$database->exec('CREATE TABLE devour_updates (target VARCHAR(255) PRIMARY KEY, time TIMESTAMP)');

		$sync = new TestSynchronizer($database, $database);
		$sync->schedule(['events', 'people'], [], 'admin@example.com', TRUE);

		$result = $database->query('SELECT tables, force FROM devour_stats LIMIT 1')->fetch(PDO::FETCH_ASSOC);

		$this->assertSame('["events","people"]', $result['tables']);
		$this->assertTrue((bool) $result['force']);
	}

	public function testRunUsesScheduledTablesAndForce(): void
	{
		$database = new PDO('sqlite::memory:');
		$database->exec('CREATE TABLE devour_stats (id INTEGER PRIMARY KEY AUTOINCREMENT, start_time TEXT, scheduled_by TEXT, scheduled_time TEXT, end_time TEXT, canceled_time TEXT, canceled_by TEXT, heartbeat TEXT, max_gap INTEGER, tables TEXT, ids TEXT, force INTEGER, log TEXT)');
		$database->exec('CREATE TABLE devour_updates (target VARCHAR(255) PRIMARY KEY, time TIMESTAMP)');

		$sync = new class($database, $database) extends TestSynchronizer {
			public $calls = [];

			protected function syncMapping($name, $ids, $force_update, $context = NULL)
			{
				$this->calls[] = compact('name', 'force_update');
			}
		};

		$sync->addMapping(new Devour\Mapping('source_events', 'events', 'id'));
		$sync->addMapping(new Devour\Mapping('source_people', 'people', 'id'));
		$sync->schedule(['events'], [], 'admin@example.com', TRUE);
		$sync->run();

		$this->assertCount(1, $sync->calls);
		$this->assertSame('events', $sync->calls[0]['name']);
		$this->assertTrue($sync->calls[0]['force_update']);
	}


	public function testStatUpdateNormalizesFalseForce(): void
	{
		$database = new PDO('sqlite::memory:');
		$database->exec('CREATE TABLE devour_stats (id INTEGER PRIMARY KEY AUTOINCREMENT, start_time TEXT, scheduled_by TEXT, scheduled_time TEXT, end_time TEXT, canceled_time TEXT, canceled_by TEXT, heartbeat TEXT, max_gap INTEGER, tables TEXT, ids TEXT, force INTEGER, log TEXT)');
		$database->exec('CREATE TABLE devour_updates (target VARCHAR(255) PRIMARY KEY, time TIMESTAMP)');
		$database->exec("INSERT INTO devour_stats (id, scheduled_time, force)
		                 VALUES (1, '2026-08-14 00:00:00', 0)");

		$sync = new class($database, $database) extends TestSynchronizer {
			public function setStat(array $stat): void
			{
				$this->stat = $stat;
			}

			public function updateStat(string $column, string $value): void
			{
				$this->statSet($column, $value);
			}
		};
		$sync->setStat([
			'id' => 1,
			'start_time' => NULL,
			'scheduled_by' => NULL,
			'scheduled_time' => '2026-08-14 00:00:00',
			'end_time' => NULL,
			'tables' => NULL,
			'ids' => NULL,
			'force' => FALSE,
			'log' => NULL
		]);
		$sync->updateStat('log', 'Updated');

		$this->assertSame(0, $database->query('SELECT force FROM devour_stats WHERE id = 1')->fetchColumn());
	}
}
