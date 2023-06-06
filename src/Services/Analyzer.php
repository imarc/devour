<?php

namespace Devour;

use PDO;
use DateTime;
use PDOException;
use RuntimeException;

/**
 *
 */
class Analyzer
{
	/**
	 *
	 */
	const LOG_PARSE = "/\[([0-9:]+)\][\s\.]+([^\s]+)\s+([0-9]*)\s*(.*)/";


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
	public function __construct(PDO $database)
	{
		$this->database = $database;
		$this->parseLogs();
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
	protected function parseLogs()
	{
		$rows = $this->database->query("
			SELECT * FROM devour_stats order by start_time desc
		");

		foreach ($rows as $result) {
			$result_data = [];
			$lines = explode("\n", $result['log']);

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
				$this->data[]        = [
					'start_time' => new DateTime($result['start_time']),
					'end_time'   => new DateTime($result['end_time']),
					'log'        => $result_data
				];

			}
		}

		$this->tables = array_unique($this->tables);
	}
}
