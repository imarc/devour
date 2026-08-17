<?php

namespace Devour;

use PDO;
use DateTime;
use PDOException;
use RuntimeException;
use Devour\Migrations\MigrationRunner;

/**
 *
 */
class Analyzer
{
	/**
	 * Log lines carry a full Y-m-d H:i:s stamp.
	 *
	 * They used to carry a 12-hour clock with no meridiem and no date, so a log spanning noon
	 * appeared to run backwards and any gap over 12 hours read as a short one.
	 */
	const LOG_PARSE = "/\[([0-9\-: ]+)\][\s\.]+([^\s]+)\s+([0-9]*)\s*(.*)/";


	/**
	 *
	 */
	const STATUS_START = 'Syncing';


	/**
	 *
	 */
	const STATUS_COUNT = 'inserting';


	/**
	 *
	 */
	const STATUS_STEP = 'completed';


	/**
	 *
	 */
	const STATUS_FAILURE = 'Failed';


	/**
	 *
	 */
	protected $data = [];


	/*
	 *
	 */
	protected $tables = [];

	/**
	 *
	 */
	protected PDO $database;


	/**
	 *
	 */
	public function __construct(PDO $database)
	{
		$this->assertMigrationReady($database);

		$this->database = $database;

		$this->parseLogs();
	}


	/**
	 * Overridable so unit tests can drive this class on SQLite.
	 */
	protected function assertMigrationReady(PDO $database): void
	{
		MigrationRunner::assertReady($database);
	}


	/**
	 *
	 */
	public function getAverageRunTime()
	{
		$data = [];
		foreach ($this->data as $result) {
			if ($result['start_time'] && $result['end_time']) {
				$data[] = $result['end_time']->format('U') - $result['start_time']->format('U');
			}
		}

		return $this->mean($data);
	}



	/**
	 *
	 */
	public function getTables()
	{
		return $this->tables;
	}



	/**
	 *
	 */
	public function getPropertyStat($property, $name, $stat = 'average')
	{
		$tables = $this->getTables();
		$data   = [];

		if (in_array($name, $tables)) {
			foreach ($this->data as $result) {
				foreach ($result['log'] as $table) {
					if ($table['table'] == $name) {
						if (isset($table[$property])) {
							$data[] = $table[$property];
						}

						break 1;
					}
				}
			}

			if ($stat == 'average') {
				return $this->mean($data);
			}
		}

		return NULL;
	}


	/**
	 *
	 */
	protected function mean(array $data) {
		return array_sum($data)/(count($data) ?: 1);
	}


	/**
	 *
	 */
	/**
	 * Build a run's entry from its recorded per-table figures.
	 *
	 * Returns NULL when the run predates table_stats, leaving it to the log parser.
	 */
	protected function readTableStats(array $result): ?array
	{
		$stats = json_decode((string) ($result['table_stats'] ?? ''), TRUE);

		if (!is_array($stats) || !$stats) {
			return NULL;
		}

		$log = [];

		foreach ($stats as $name => $figures) {
			$this->tables[] = $name;

			$log[] = [
				'table'      => $name,
				'start_time' => $figures['start'] ? new DateTime($figures['start']) : NULL,
				'end_time'   => $figures['end'] ? new DateTime($figures['end']) : NULL,
				'duration'   => $figures['duration'] ?? NULL,
				'count'      => $figures['updated'] ?? NULL,
				'step'       => 'completed',
				'failures'   => array_fill(0, (int) ($figures['failed'] ?? 0), 'failed'),
			];
		}

		return [
			'start_time' => new DateTime($result['start_time']),
			'end_time'   => $result['end_time'] ? new DateTime($result['end_time']) : NULL,
			'log'        => $log
		];
	}


	/**
	 *
	 */
	protected function parseLogs()
	{
		$rows = $this->database->query("
			SELECT * FROM devour_stats order by start_time desc
		");

		foreach ($rows as $result) {
			//
			// Runs recorded since table_stats existed carry their figures as data.  Older runs, and
			// only those, still have them recovered from the log prose below — which is why that
			// parsing stays here rather than being removed with the verbose logging it reads.
			//
			if ($structured = $this->readTableStats($result)) {
				$this->data[] = $structured;

				continue;
			}

			$result_data = [];
			$lines       = explode("\n", (string) $result['log']);

			//
			// $table carried across rows before this, so a run whose log parsed to nothing
			// inherited the previous run's tables and reported them under the wrong run.
			//
			unset($table);

			foreach ($lines as $line) {
				$parse = [];
				if (preg_match(self::LOG_PARSE, $line, $parse)) {
					$parsed_data = [
						'time'   => $parse[1] ?? NULL,
						'status' => $parse[2] ?? NULL,
						'amount' => $parse[3] ?? NULL,
						'target' => $parse[4] ?? NULL
					];

					if ($parsed_data['status'] == self::STATUS_START) {
						if (isset($table)) {
							$result_data[] = $table;
						}

						$this->tables[] = $parsed_data['target'];

						$table = [
							'table'      => $parsed_data['target'],
							'start_time' => new DateTime($parsed_data['time']),
							'end_time'   => NULL,
							'duration'   => NULL,
							'count'      => NULL,
							'step'       => $parsed_data['status'],
							'failures'   => []
						];
					}

					if ($parsed_data['status'] == self::STATUS_COUNT) {
						$table['count'] = $parsed_data['amount'];
					}

					if ($parsed_data['status'] == self::STATUS_STEP) {
						$table['step']     = $parsed_data['target'];
						$table['end_time'] = new DateTime($parsed_data['time']);
						if ($table['start_time']->format('U') < $table['end_time']->format('U')) {
							$table['duration'] = $table['end_time']->format('U') - $table['start_time']->format('U');
						}
					}

					if ($parsed_data['status'] == self::STATUS_FAILURE) {
						$table['failures'][] = $parsed_data['target'];
					}
				}
			}

			if (isset($table)) {
				$result_data[] = $table;
				$this->data[] = [
					'start_time' => new DateTime($result['start_time']),
					//
					// An open run has no end time.  Passing NULL here yielded "now", so a sync
					// still in flight reported a completed duration.
					//
					'end_time'   => $result['end_time'] ? new DateTime($result['end_time']) : NULL,
					'log'        => $result_data
				];

			}
		}

		$this->tables = array_unique($this->tables);
	}
}
