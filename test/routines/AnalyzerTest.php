<?php

use PHPUnit\Framework\TestCase;

class TestAnalyzer extends Devour\Analyzer
{
	protected function assertMigrationReady(PDO $database): void
	{
	}
}

/**
 * Counts the queries the analyzer issues, so tests can assert when devour_stats is read.
 */
class CountingPDO extends PDO
{
	public int $queries = 0;

	public function query(string $query, ?int $fetchMode = null, mixed ...$fetchModeArgs): PDOStatement|false
	{
		$this->queries++;

		return parent::query($query, $fetchMode, ...$fetchModeArgs);
	}
}

final class AnalyzerTest extends TestCase
{
	private function database(array $rows): PDO
	{
		return $this->seed(new PDO('sqlite::memory:'), $rows);
	}


	private function countingDatabase(array $rows): CountingPDO
	{
		return $this->seed(new CountingPDO('sqlite::memory:'), $rows);
	}


	private function seed(PDO $database, array $rows): PDO
	{
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


	/**
	 * Constructing the analyzer must not read devour_stats.
	 *
	 * Only sync:stats wants these figures, but the analyzer is built through the DI container
	 * whenever a console command is registered, so parsing in the constructor made every command
	 * pay for it. A site whose devour_stats still held pre-SUMMARY verbose logs exhausted memory
	 * before any command could run.
	 */
	public function testConstructionDoesNotReadTheStatsTable()
	{
		$database = $this->countingDatabase([
			['2026-08-17 09:00:00', '2026-08-17 09:10:00', "[2026-08-17 09:00:00] Syncing events\n"],
		]);

		$queries = $database->queries;

		new TestAnalyzer($database);

		$this->assertSame($queries, $database->queries);
	}


	/**
	 * The logs are parsed on first use, and only once no matter how many accessors are called.
	 */
	public function testStatsAreParsedOnceOnFirstUse()
	{
		$database = $this->countingDatabase([
			['2026-08-17 09:00:00', '2026-08-17 09:10:00',
			 "[2026-08-17 09:00:00] Syncing events\n[2026-08-17 09:05:00] ...completed inserts\n"],
		]);

		$analyzer = new TestAnalyzer($database);
		$queries  = $database->queries;

		$this->assertSame(['events'], array_values($analyzer->getTables()));
		$this->assertSame(600, (int) $analyzer->getAverageRunTime());
		$this->assertNull($analyzer->getPropertyStat('duration', 'absent'));

		$this->assertSame($queries + 1, $database->queries);
	}
}
