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
			errors TEXT, table_stats TEXT,
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

		$sync->setLogVerbosity(Devour\Synchronizer::VERBOSITY_VERBOSE);
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


	private function loggingSynchronizer(PDO $database): object
	{
		$sync = new class($database, $database) extends TestSynchronizer {
			public function emit(string $message, int $level): void
			{
				$this->log($message, $level);
			}

			public function emitError(string $table, string $op, string $message, $context = NULL): void
			{
				$this->logError($table, $op, $message, $context);
			}

			public function emitErrorFor(string $table, string $op, string $message, string $id): void
			{
				$this->logError($table, $op, $message, NULL, $id);
			}

			public function identifierFor(Devour\Mapping $mapping, array $row): ?string
			{
				return $this->composeRowIdentifier($mapping, $row);
			}

			public function openStats(string $table): void
			{
				$this->openTableStats($table);
			}

			public function countStat(string $table, string $metric, int $n): void
			{
				$this->countTableStat($table, $metric, $n);
			}

			public function closeStats(string $table): void
			{
				$this->closeTableStats($table);
			}
		};

		// keep stdout quiet; the stored log is what these assert on
		$sync->setEchoVerbosity(-1);

		return $sync;
	}


	public function testSummaryVerbosityKeepsSummariesAndErrorsButNotProgress()
	{
		$database = $this->statsDatabase();
		$sync     = $this->loggingSynchronizer($database);

		$sync->schedule([], [], 'admin@example.com');
		$sync->emit('...transfering 500 temporary records', Devour\Synchronizer::VERBOSITY_VERBOSE);
		$sync->emit('[events] 500 transferred in 3s', Devour\Synchronizer::VERBOSITY_SUMMARY);
		$sync->emitError('events', 'insert failed', 'duplicate key');

		$log = (string) $database->query('SELECT log FROM devour_stats LIMIT 1')->fetchColumn();

		$this->assertStringNotContainsString('transfering', $log);
		$this->assertStringContainsString('500 transferred in 3s', $log);
		$this->assertStringContainsString('duplicate key', $log);
	}


	public function testVerboseVerbosityKeepsEverything()
	{
		$database = $this->statsDatabase();
		$sync     = $this->loggingSynchronizer($database);

		$sync->setLogVerbosity(Devour\Synchronizer::VERBOSITY_VERBOSE);
		$sync->schedule([], [], 'admin@example.com');
		$sync->emit('...transfering 500 temporary records', Devour\Synchronizer::VERBOSITY_VERBOSE);

		$this->assertStringContainsString(
			'transfering',
			(string) $database->query('SELECT log FROM devour_stats LIMIT 1')->fetchColumn()
		);
	}


	/**
	 * A failing batch repeats one problem for every row; the column records it once with a count.
	 */
	public function testErrorsAggregateByTableAndMessage()
	{
		$database = $this->statsDatabase();
		$sync     = $this->loggingSynchronizer($database);

		$sync->schedule([], [], 'admin@example.com');

		foreach (['pb', 'pc', 'at'] as $id) {
			$sync->emitError('ledgers', 'insert failed', 'duplicate key on ledgers_pkey', '{"id":"' . $id . '"}');
		}

		$sync->emitError('events', 'update failed', 'value too long', '{"id":"e1"}');

		$error = (string) $database->query('SELECT errors FROM devour_stats LIMIT 1')->fetchColumn();
		$lines = array_values(array_filter(explode("\n", $error)));

		$this->assertCount(2, $lines);
		$this->assertStringContainsString('[ledgers] 3 x insert failed: duplicate key on ledgers_pkey', $lines[0]);
		$this->assertStringContainsString('[events] update failed: value too long', $lines[1]);
	}


	/**
	 * Otherwise-identical failures differing only by the offending value are one error.
	 */
	/**
	 * PostgreSQL names the offending value in DETAIL, which made every row its own error.
	 */
	public function testErrorsAggregateAcrossPostgresDetailValues()
	{
		$database = $this->statsDatabase();
		$sync     = $this->loggingSynchronizer($database);

		$sync->schedule([], [], 'admin@example.com');

		foreach (['pb', 'pc', 'at', 'px', 'nc'] as $id) {
			$sync->emitErrorFor(
				'ledgers',
				'transfer failed',
				'SQLSTATE[23505]: Unique violation: 7 ERROR: duplicate key value violates unique '
					. 'constraint "devour_temp_ledgers_pkey" DETAIL: Key (id)=(' . $id . ') already exists.',
				$id
			);
		}

		$errors = (string) $database->query('SELECT errors FROM devour_stats LIMIT 1')->fetchColumn();

		$this->assertCount(1, array_filter(explode("\n", $errors)));
		$this->assertStringContainsString('5 x transfer failed', $errors);
		$this->assertStringContainsString('Key (id)=(?) already exists', $errors);
		$this->assertStringContainsString('affected records: pb, pc, at, px, nc', $errors);
	}


	/**
	 * A width is part of the type, not a value, so violations on different columns stay distinct.
	 */
	public function testErrorsKeepStructuralNumbersDistinct()
	{
		$database = $this->statsDatabase();
		$sync     = $this->loggingSynchronizer($database);

		$sync->schedule([], [], 'admin@example.com');
		$sync->emitErrorFor('people', 'insert failed', 'value too long for type character varying(5)', 'a');
		$sync->emitErrorFor('people', 'insert failed', 'value too long for type character varying(50)', 'b');

		$errors = (string) $database->query('SELECT errors FROM devour_stats LIMIT 1')->fetchColumn();

		$this->assertCount(2, array_filter(explode("\n", $errors)));
		$this->assertStringContainsString('varying(5)', $errors);
		$this->assertStringContainsString('varying(50)', $errors);
	}


	public function testErrorsAggregateAcrossVaryingValues()
	{
		$database = $this->statsDatabase();
		$sync     = $this->loggingSynchronizer($database);

		$sync->schedule([], [], 'admin@example.com');
		$sync->emitError('people', 'insert failed', "invalid input syntax for integer: 'abc'");
		$sync->emitError('people', 'insert failed', "invalid input syntax for integer: 'xyz'");

		$error = (string) $database->query('SELECT errors FROM devour_stats LIMIT 1')->fetchColumn();

		$this->assertCount(1, array_filter(explode("\n", $error)));
		$this->assertStringContainsString('2 x insert failed', $error);
	}


	/**
	 * The keys of every affected record survive aggregation.
	 *
	 * These are what someone works from to correct records at the source, so keeping one example
	 * and discarding the rest loses the actionable part of the error.
	 */
	public function testAggregatedErrorsListEveryAffectedRecord()
	{
		$database = $this->statsDatabase();
		$sync     = $this->loggingSynchronizer($database);

		$sync->schedule([], [], 'admin@example.com');

		foreach (['603523is', '603552erb', '603553erb'] as $id) {
			$sync->emitErrorFor('events', 'insert failed', 'null value in column "category"', $id);
		}

		$error = (string) $database->query('SELECT errors FROM devour_stats LIMIT 1')->fetchColumn();

		$this->assertStringContainsString('3 x insert failed', $error);
		$this->assertStringContainsString('affected records: 603523is, 603552erb, 603553erb', $error);
	}


	/**
	 * When the key itself is what failed, there is no identifier — so the row is shown instead.
	 */
	public function testErrorsFallBackToTheRowWhenTheKeyIsUnavailable()
	{
		$database = $this->statsDatabase();
		$sync     = $this->loggingSynchronizer($database);

		$sync->schedule([], [], 'admin@example.com');
		$sync->emitError('committees', 'insert failed', 'null value in column "id"', '{"id":null,"name":"Audit"}');

		$error = (string) $database->query('SELECT errors FROM devour_stats LIMIT 1')->fetchColumn();

		$this->assertStringContainsString('(e.g. {"id":null,"name":"Audit"})', $error);
	}


	public function testRepeatedFailuresOfOneRecordAreListedOnce()
	{
		$database = $this->statsDatabase();
		$sync     = $this->loggingSynchronizer($database);

		$sync->schedule([], [], 'admin@example.com');
		$sync->emitErrorFor('events', 'insert failed', 'duplicate key', '603523is');
		$sync->emitErrorFor('events', 'insert failed', 'duplicate key', '603523is');

		$error = (string) $database->query('SELECT errors FROM devour_stats LIMIT 1')->fetchColumn();

		$this->assertSame(1, substr_count($error, '603523is'));
		$this->assertStringContainsString('affected record: 603523is', $error);
	}


	public function testAffectedRecordListIsCapped()
	{
		$database = $this->statsDatabase();
		$sync     = $this->loggingSynchronizer($database);

		$sync->schedule([], [], 'admin@example.com');

		for ($i = 0; $i < Devour\Synchronizer::IDENTIFIER_LIMIT + 5; $i++) {
			$sync->emitErrorFor('events', 'insert failed', 'duplicate key', 'id' . $i);
		}

		$error = (string) $database->query('SELECT errors FROM devour_stats LIMIT 1')->fetchColumn();

		$this->assertStringContainsString('(+5 more)', $error);
	}


	/**
	 * A composite key names its parts; the values alone would not identify anything.
	 */
	public function testCompositeKeysAreNamedInTheAffectedList()
	{
		$database = $this->statsDatabase();
		$sync     = $this->loggingSynchronizer($database);

		$sessions = new Devour\Mapping('evses', 'event_sessions', ['code', 'event']);

		$this->assertSame(
			'code=A event=603523is',
			$sync->identifierFor($sessions, ['code' => 'A', 'event' => '603523is'])
		);

		$events = new Devour\Mapping('evmas', 'events', 'id');

		$this->assertSame('603523is', $sync->identifierFor($events, ['id' => '603523is']));
		$this->assertNull($sync->identifierFor($events, ['id' => NULL]));
	}


	public function testTableStatsAreRecordedStructurally()
	{
		$database = $this->statsDatabase();
		$sync     = $this->loggingSynchronizer($database);

		$sync->schedule([], [], 'admin@example.com');
		$sync->openStats('events');
		$sync->countStat('events', 'transferred', 500);
		$sync->countStat('events', 'inserted', 3);
		$sync->countStat('events', 'updated', 44);
		$sync->emitError('events', 'insert failed', 'duplicate key');
		$sync->closeStats('events');

		$stats = json_decode(
			(string) $database->query('SELECT table_stats FROM devour_stats LIMIT 1')->fetchColumn(),
			TRUE
		);

		$this->assertSame(500, $stats['events']['transferred']);
		$this->assertSame(3,   $stats['events']['inserted']);
		$this->assertSame(44,  $stats['events']['updated']);
		$this->assertSame(1,   $stats['events']['failed']);
		$this->assertNotNull($stats['events']['end']);

		$this->assertStringContainsString(
			'[events] 500 transferred, 3 inserted, 44 updated, 1 failed',
			(string) $database->query('SELECT log FROM devour_stats LIMIT 1')->fetchColumn()
		);
	}


	/**
	 * A second run must not report the first run's failures against its own row.
	 */
	public function testRunClearsAccumulatorsFromAnEarlierRun()
	{
		$database = $this->statsDatabase();

		$sync = new class($database, $database) extends TestSynchronizer {
			public function emitError(string $table, string $message): void
			{
				$this->logError($table, 'insert failed', $message, NULL, 'first-run-record');
			}
		};

		$sync->setEchoVerbosity(-1);
		$sync->schedule([], [], 'admin@example.com');
		$sync->emitError('events', 'duplicate key');
		$sync->run();

		$first = (int) $database->query('SELECT id FROM devour_stats ORDER BY id DESC LIMIT 1')->fetchColumn();

		$this->assertStringContainsString(
			'first-run-record',
			(string) $database->query('SELECT errors FROM devour_stats WHERE id = ' . $first)->fetchColumn()
		);

		// a second run against the same instance opens a new row, which then fails on its own
		$sync->run();

		$second = (int) $database->query('SELECT id FROM devour_stats ORDER BY id DESC LIMIT 1')->fetchColumn();

		$this->assertNotSame($first, $second);

		$sync->emitError('people', 'value too long');

		$error = (string) $database->query('SELECT errors FROM devour_stats WHERE id = ' . $second)->fetchColumn();

		$this->assertStringContainsString('value too long', $error);
		$this->assertStringNotContainsString('duplicate key', $error);
	}


	public function testRunStopsWhenCancelledMidway()
	{
		$database = $this->statsDatabase();

		$sync = new class($database, $database) extends TestSynchronizer {
			public array $syncedNames = [];

			protected function syncMapping($name, $ids, $force_update, $context = NULL)
			{
				//
				// Logged before recording, so syncedNames holds only the mappings that got past
				// their first log line — which is where a cancelled run is meant to stop.
				//
				$this->log('syncing ' . $name);

				$this->syncedNames[] = $name;

				if ($name === 'events') {
					$this->destination->exec(
						"UPDATE devour_stats SET canceled_time = '2026-08-17 09:00:00'"
					);
				}
			}
		};

		$sync->addMapping(new Devour\Mapping('source_events', 'events', 'id'));
		$sync->addMapping(new Devour\Mapping('source_people', 'people', 'id'));
		$sync->run();

		$row = $database->query('SELECT * FROM devour_stats LIMIT 1')->fetch(PDO::FETCH_ASSOC);

		// events ran and cancelled itself; people must never have been attempted
		$this->assertSame(['events'], $sync->syncedNames);
		$this->assertNull($row['end_time']);
		$this->assertNotNull($row['canceled_time']);
	}


	public function testUpdateSetWritesTheTimestampItWasGiven()
	{
		$database = $this->statsDatabase();
		$database->exec("INSERT INTO devour_updates (target, time) VALUES ('people', '1800-01-01 00:00:00')");

		$sync = new TestSynchronizer($database, $database);
		$sync->updateSet('people', '2026-08-17 09:00:00');

		$this->assertSame(
			'2026-08-17 09:00:00',
			$database->query("SELECT time FROM devour_updates WHERE target = 'people'")->fetchColumn()
		);
	}


	public function testIsRunningIgnoresCancelledRows()
	{
		$database = $this->statsDatabase();
		$database->exec("INSERT INTO devour_stats (id, start_time, canceled_time)
		                 VALUES (1, '2026-08-17 09:00:00', '2026-08-17 09:30:00')");

		$sync = new TestSynchronizer($database, $database);

		$this->assertFalse($sync->isRunning());
	}


	private function dedupSynchronizer(): object
	{
		$database = $this->statsDatabase();

		$sync = new class($database, $database) extends TestSynchronizer {
			public function callUnsynced($name, $ids)
			{
				return $this->unsynced($name, $ids);
			}

			public function callRecordSynced($name, $ids): void
			{
				$this->recordSynced($name, $ids);
			}

			public function syncedFor($name)
			{
				return $this->synced[$name];
			}
		};

		// event_sessions keys on (code, event), like most adjunct tables
		$sync->addMapping(new Devour\Mapping('evses', 'event_sessions', ['code', 'event']));
		$sync->addMapping(new Devour\Mapping('evfee', 'event_fees', 'id'));

		return $sync;
	}


	/**
	 * The second subset sync of a composite-key mapping used to read an undefined 'id' offset.
	 */
	public function testSubsetDedupHandlesCompositeKeys()
	{
		$sync = $this->dedupSynchronizer();

		$sync->callRecordSynced('event_sessions', [
			['code' => 'A', 'event' => '1'],
			['code' => 'B', 'event' => '1'],
		]);

		$remaining = $sync->callUnsynced('event_sessions', [
			['code' => 'A', 'event' => '1'],   // already synced
			['code' => 'B', 'event' => '2'],   // same code, different event
			['code' => 'C', 'event' => '1'],   // new
		]);

		$this->assertSame(
			[['code' => 'B', 'event' => '2'], ['code' => 'C', 'event' => '1']],
			array_values($remaining)
		);
	}


	public function testSubsetDedupStillHandlesSingleKeys()
	{
		$sync = $this->dedupSynchronizer();

		$sync->callRecordSynced('event_fees', [['id' => '10']]);

		$remaining = $sync->callUnsynced('event_fees', [['id' => '10'], ['id' => '11']]);

		$this->assertSame([['id' => '11']], array_values($remaining));
	}


	/**
	 * Two events synced in one run each contribute a batch of adjunct keys.
	 */
	public function testSubsetResultsAccumulateAcrossBatches()
	{
		$sync = $this->dedupSynchronizer();

		$sync->callRecordSynced('event_sessions', [['code' => 'A', 'event' => '1']]);
		$sync->callRecordSynced('event_sessions', [['code' => 'A', 'event' => '2']]);

		$this->assertCount(2, $sync->syncedFor('event_sessions'));

		$remaining = $sync->callUnsynced('event_sessions', [
			['code' => 'A', 'event' => '1'],
			['code' => 'A', 'event' => '2'],
		]);

		$this->assertSame([], $remaining);
	}


	public function testFullSyncMarksAMappingEntirelySynced()
	{
		$sync = $this->dedupSynchronizer();

		$sync->callRecordSynced('event_sessions', []);

		$this->assertTrue($sync->syncedFor('event_sessions'));
		$this->assertFalse($sync->callUnsynced('event_sessions', [['code' => 'A', 'event' => '1']]));
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
