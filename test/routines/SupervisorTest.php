<?php

use PHPUnit\Framework\TestCase;

class TestSupervisor extends Devour\Supervisor
{
	protected function assertMigrationReady(PDO $database): void
	{
	}
}

final class SupervisorTest extends TestCase
{
	protected function database(array $rows = []): PDO
	{
		$database = new PDO('sqlite::memory:');
		$database->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
		$database->exec('CREATE TABLE devour_stats (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			start_time TEXT, scheduled_by TEXT, scheduled_time TEXT, end_time TEXT,
			canceled_time TEXT, canceled_by TEXT, heartbeat TEXT, max_gap INTEGER,
			tables TEXT, ids TEXT, force INTEGER, log TEXT
		)');

		$insert = $database->prepare('INSERT INTO devour_stats
			(id, start_time, scheduled_by, scheduled_time, end_time, canceled_time, canceled_by,
			 heartbeat, max_gap, tables, ids, force, log)
			VALUES (:id, :start_time, :scheduled_by, :scheduled_time, :end_time, :canceled_time,
			        :canceled_by, :heartbeat, :max_gap, :tables, :ids, :force, :log)');

		foreach ($rows as $row) {
			$insert->execute($row + [
				'id' => NULL, 'start_time' => NULL, 'scheduled_by' => NULL, 'scheduled_time' => NULL,
				'end_time' => NULL, 'canceled_time' => NULL, 'canceled_by' => NULL,
				'heartbeat' => NULL, 'max_gap' => NULL, 'tables' => NULL, 'ids' => NULL,
				'force' => 0, 'log' => NULL,
			]);
		}

		return $database;
	}


	public function testFindRunningExcludesScheduledCompleteAndCancelled()
	{
		$database = $this->database([
			['id' => 1, 'start_time' => '2026-08-17 09:00:00'],
			['id' => 2, 'scheduled_time' => '2026-08-17 09:00:00'],
			['id' => 3, 'start_time' => '2026-08-17 08:00:00', 'end_time' => '2026-08-17 08:30:00'],
			['id' => 4, 'start_time' => '2026-08-17 07:00:00', 'canceled_time' => '2026-08-17 07:30:00'],
			['id' => 5, 'start_time' => '2026-08-17 10:00:00'],
		]);

		$ids = array_map(
			function ($run) { return $run->getId(); },
			(new TestSupervisor($database))->findRunning()
		);

		$this->assertSame([5, 1], $ids);
	}


	public function testGapBaselineIsScopedToContextAndCompletedRuns()
	{
		$database = $this->database([
			// full syncs
			['start_time' => '2026-08-01 00:00:00', 'end_time' => '2026-08-01 01:00:00', 'max_gap' => 120],
			['start_time' => '2026-08-02 00:00:00', 'end_time' => '2026-08-02 01:00:00', 'max_gap' => 300],
			// a still-running full sync must not contribute
			['start_time' => '2026-08-03 00:00:00', 'max_gap' => 9999],
			// a limited sync must not contribute to the full baseline
			['start_time' => '2026-08-04 00:00:00', 'end_time' => '2026-08-04 00:10:00',
			 'tables' => '["people"]', 'max_gap' => 45],
		]);

		$supervisor = new TestSupervisor($database);

		$this->assertSame(300, $supervisor->getGapBaseline(NULL));
		$this->assertSame(45,  $supervisor->getGapBaseline('limited'));
		$this->assertNull($supervisor->getGapBaseline('individual'));
	}


	public function testFindRunningAttachesTheMatchingBaseline()
	{
		$database = $this->database([
			['start_time' => '2026-08-01 00:00:00', 'end_time' => '2026-08-01 01:00:00', 'max_gap' => 300],
			['start_time' => '2026-08-02 00:00:00', 'end_time' => '2026-08-02 00:10:00',
			 'tables' => '["people"]', 'max_gap' => 45],
			['start_time' => '2026-08-03 00:00:00'],
			['start_time' => '2026-08-03 00:00:00', 'tables' => '["people"]'],
		]);

		$running   = (new TestSupervisor($database))->findRunning();
		$baselines = array_map(function ($run) { return $run->getBaseline(); }, $running);

		// newest first: the limited run, then the full run
		$this->assertSame([45, 300], $baselines);
	}


	public function testFindReturnsAnyRowInAnyState()
	{
		$database = $this->database([
			['id' => 1, 'start_time' => '2026-08-01 00:00:00', 'end_time' => '2026-08-01 01:00:00'],
		]);

		$supervisor = new TestSupervisor($database);

		$this->assertSame(1, $supervisor->find(1)->getId());
		$this->assertNull($supervisor->find(99));
	}


	/**
	 * Pins Run::WHERE_RUNNING against Run::isRunning() over one shared fixture set.
	 *
	 * The predicate necessarily exists twice — SQL cannot call a PHP method — so this is the test
	 * that fails if someone changes one representation and forgets the other.
	 */
	public function testSqlAndPhpRunningPredicatesAgree()
	{
		$database = $this->database([
			['id' => 1, 'start_time' => '2026-08-17 09:00:00'],
			['id' => 2, 'scheduled_time' => '2026-08-17 09:00:00'],
			['id' => 3, 'start_time' => '2026-08-17 08:00:00', 'end_time' => '2026-08-17 08:30:00'],
			['id' => 4, 'start_time' => '2026-08-17 07:00:00', 'canceled_time' => '2026-08-17 07:30:00'],
			['id' => 5, 'start_time' => '2026-08-17 06:00:00', 'end_time' => '2026-08-17 06:30:00',
			 'canceled_time' => '2026-08-17 06:15:00'],
		]);

		$sql = $database
			->query(sprintf('SELECT id FROM devour_stats WHERE %s ORDER BY id', Devour\Run::WHERE_RUNNING))
			->fetchAll(PDO::FETCH_COLUMN)
		;

		$php = [];

		foreach ($database->query('SELECT * FROM devour_stats ORDER BY id')->fetchAll(PDO::FETCH_ASSOC) as $row) {
			if ((new Devour\Run($row))->isRunning()) {
				$php[] = $row['id'];
			}
		}

		$this->assertSame(array_map('intval', $sql), array_map('intval', $php));
		$this->assertSame([1], array_map('intval', $php));
	}
}
