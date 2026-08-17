<?php

use PHPUnit\Framework\TestCase;

class TestAnalyzer extends Devour\Analyzer
{
	protected function assertMigrationReady(PDO $database): void
	{
	}
}

final class AnalyzerTest extends TestCase
{
	private function database(array $rows): PDO
	{
		$database = new PDO('sqlite::memory:');
		$database->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
		$database->exec('CREATE TABLE devour_stats (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			start_time TEXT, scheduled_by TEXT, scheduled_time TEXT, end_time TEXT,
			canceled_time TEXT, canceled_by TEXT, heartbeat TEXT, max_gap INTEGER,
			tables TEXT, ids TEXT, force INTEGER, log TEXT
		)');

		$insert = $database->prepare(
			'INSERT INTO devour_stats (start_time, end_time, log) VALUES (?, ?, ?)'
		);

		foreach ($rows as $row) {
			$insert->execute($row);
		}

		return $database;
	}


	/**
	 * A run whose log parses to nothing must not inherit the previous run's tables.
	 *
	 * With the leak, the second row produces a data entry carrying the first row's table, so its
	 * duration is averaged in as though it had been parsed.
	 */
	public function testTablesDoNotLeakBetweenRuns()
	{
		$database = $this->database([
			['2026-08-17 09:00:00', '2026-08-17 09:10:00',
			 "[2026-08-17 09:00:00] Syncing events\n[2026-08-17 09:05:00] ...completed inserts\n"],
			['2026-08-17 10:00:00', '2026-08-17 10:30:00', "nothing parseable here\n"],
		]);

		// only the first row parses, so only its 600s duration is averaged
		$this->assertSame(600, (int) (new TestAnalyzer($database))->getAverageRunTime());
	}


	public function testTablesAreCollectedFromParsedLogs()
	{
		$database = $this->database([
			['2026-08-17 09:00:00', '2026-08-17 09:10:00',
			 "[2026-08-17 09:00:00] Syncing events\n[2026-08-17 09:05:00] ...completed inserts\n"],
		]);

		$this->assertSame(['events'], array_values((new TestAnalyzer($database))->getTables()));
	}


	/**
	 * An open run has no end_time, so it contributes no duration.
	 *
	 * Passing NULL to the DateTime constructor yields "now", which made a still-running sync report
	 * a completed duration measured from whenever the analyzer happened to run.
	 */
	public function testOpenRunsDoNotReportACompletedDuration()
	{
		$database = $this->database([
			['2026-08-17 09:00:00', NULL, "[2026-08-17 09:00:00] Syncing events\n"],
		]);

		$this->assertSame(0, (int) (new TestAnalyzer($database))->getAverageRunTime());
	}
}
