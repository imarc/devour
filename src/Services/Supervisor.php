<?php

namespace Devour;

use PDO;
use Devour\Migrations\MigrationRunner;

/**
 * Reads and cancels sync runs.
 *
 * Takes only the destination connection, like Analyzer, so an admin action or a console command can
 * inspect and stop a run without constructing a fully-mapped Synchronizer.
 */
class Supervisor
{
	/**
	 * Historical buckets, keyed by Run::getContext().
	 *
	 * Mirrors Synchronizer::getSyncInterval() so a run is only ever compared against runs of its
	 * own shape.
	 */
	const CONTEXTS = [
		'individual' => 'tables IS NOT NULL AND ids IS NOT NULL',
		'limited'    => 'tables IS NOT NULL AND ids IS NULL',
		''           => 'tables IS NULL AND ids IS NULL',
	];


	/**
	 *
	 */
	protected PDO $database;


	/**
	 * @var array<string, int|null>
	 */
	protected array $baselines = [];


	/**
	 *
	 */
	public function __construct(PDO $database)
	{
		$this->assertMigrationReady($database);

		$this->database = $database;
	}


	/**
	 * Overridable so unit tests can drive this class on SQLite.
	 */
	protected function assertMigrationReady(PDO $database): void
	{
		MigrationRunner::assertReady($database);
	}


	/**
	 * Every run that has started and neither finished nor been cancelled, newest first.
	 *
	 * The baseline is resolved once per distinct context across the whole result set — never per
	 * row, which would be an N+1 on a screen that renders a list.
	 *
	 * @return Run[]
	 */
	public function findRunning(): array
	{
		$statement = $this->database->query(sprintf(
			'SELECT * FROM devour_stats WHERE %s ORDER BY id DESC',
			Run::WHERE_RUNNING
		));

		$runs = [];

		foreach ($statement->fetchAll(PDO::FETCH_ASSOC) as $row) {
			$run    = new Run($row);
			$runs[] = $run->withBaseline($this->getGapBaseline($run->getContext()));
		}

		return $runs;
	}


	/**
	 *
	 */
	public function find(int $id): ?Run
	{
		$statement = $this->database->prepare('SELECT * FROM devour_stats WHERE id = :id');
		$statement->execute(['id' => $id]);

		$row = $statement->fetch(PDO::FETCH_ASSOC);

		if (!$row) {
			return NULL;
		}

		$run = new Run($row);

		return $run->withBaseline($this->getGapBaseline($run->getContext()));
	}


	/**
	 * The largest gap between log lines ever recorded by a completed run of this shape.
	 *
	 * Completed runs only.  A run that died stopped writing lines, so the gap that killed it was
	 * never recorded — the baseline cannot be contaminated by the very stalls it is used to detect.
	 */
	public function getGapBaseline(?string $context): ?int
	{
		$key = (string) $context;

		if (array_key_exists($key, $this->baselines)) {
			return $this->baselines[$key];
		}

		$where = self::CONTEXTS[$key] ?? self::CONTEXTS[''];

		$result = $this->database->query(sprintf(
			'SELECT MAX(max_gap) AS baseline FROM devour_stats
			 WHERE end_time IS NOT NULL AND max_gap IS NOT NULL AND %s',
			$where
		))->fetch(PDO::FETCH_ASSOC);

		$baseline = ($result && $result['baseline'] !== NULL)
			? (int) $result['baseline']
			: NULL;

		return $this->baselines[$key] = $baseline;
	}
}
