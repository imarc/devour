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
class Synchronizer
{
	const SLEEP_TIME  = 0;


	/**
	 * Every devour_stats column Synchronizer writes through statSet().
	 *
	 * Listed once so INSERT and UPDATE cannot drift apart, and so adding a column is one edit
	 * rather than two.  heartbeat and max_gap are absent deliberately: they are maintained by
	 * logAppend(), not by statSet().
	 */
	/**
	 * Errors only.  Suitable for a scheduled run where only failures are worth keeping.
	 */
	const VERBOSITY_ERRORS = 0;


	/**
	 * A line per table, plus errors.
	 */
	const VERBOSITY_SUMMARY = 1;


	/**
	 * Step-by-step progress within each table.
	 */
	const VERBOSITY_VERBOSE = 2;


	/**
	 * Adds the offending rows and queries behind each error.
	 */
	const VERBOSITY_DEBUG = 3;


	/**
	 * How many distinct errors the error column holds before it starts counting rather than listing.
	 */
	const ERROR_LIMIT = 50;


	/**
	 * How many recent completed runs an interval estimate is drawn from.
	 *
	 * Counted in runs rather than measured in days so that a site syncing weekly still has a
	 * usable sample.
	 */
	const INTERVAL_SAMPLE = 20;


	const STAT_COLUMNS = [
		'start_time',
		'scheduled_by',
		'scheduled_time',
		'end_time',
		'tables',
		'ids',
		'force',
		'log',
	];

	/**
	 *
	 */
	protected $chunkLimit = NULL;


	/**
	 *
	 */
	protected $destination = NULL;


	/**
	 *
	 */
	protected $filters = array();


	/**
	 *
	 */
	protected $generators = array();


	/**
	 *
	 */
	protected $source = NULL;


	/**
	 *
	 */
	protected $stack = array();


	/**
	 *
	 */
	protected $stat = array();


	/**
	 * Aggregated failures, keyed by table + operation + normalized message.
	 */
	protected $errors = array();


	/**
	 * Per-table figures, keyed by destination table.
	 */
	protected $tableStats = array();


	/**
	 *
	 */
	protected $logVerbosity = self::VERBOSITY_SUMMARY;


	/**
	 *
	 */
	protected $echoVerbosity = self::VERBOSITY_VERBOSE;


	/**
	 *
	 */
	protected $synced = array();


	/**
	 *
	 */
	protected $mappings = array();


	/**
	 *
	 */
	protected $strictTime = NULL;


	/**
	 *
	 */
	protected $truncate = array();


	/**
	 * 
	 */
	protected $pruneStatsWhere = array();


	/**
	 *
	 */
	public function __construct(PDO $source, PDO $destination, $strict_time = FALSE, $chunk_limit = 5000)
	{
		$this->assertMigrationReady($destination);
		$this->source      = $source;
		$this->destination = $destination;
		$this->strictTime  = $strict_time;
		$this->chunkLimit  = $chunk_limit;
	}


	/**
	 *
	 */
	public function addMapping(Mapping $mapping, $truncate = FALSE)
	{
		$destination = $mapping->getDestination();

		$this->mappings[$destination] = $mapping;
		$this->truncate[$destination] = $truncate;
		$this->synced[$destination]   = FALSE;
	}


	/**
	 *
	 */
	public function addFilter($name, callable $filter)
	{
		$this->filters[$name] = $filter;
	}



	/**
	 *
	 */
	public function addGenerator($name, callable $generator)
	{
		$this->generators[$name] = $generator;
	}


	protected function assertMigrationReady(PDO $database): void
	{
		MigrationRunner::assertReady($database);
	}


	/**
	 * 
	 */
	public function createTemporaryTable($mapping)
	{
		$this->destination->query(sprintf("
			CREATE TEMPORARY TABLE devour_temp_%s (LIKE %s INCLUDING ALL, devour_updated bool default true %s)
			",
			$mapping->getDestination(),
			$mapping->getDestination(),
			count($mapping->getContextFields()) ? ', ' . join(', ', array_map(function($alias) {
				return $alias . ' varchar ';
			}, array_keys($mapping->getContextFields()))) : ''
		));
	}


	/**
	 * 
	 */
	public function getOpenRun(): ?Run
	{
		$row = $this->destination
			->query(sprintf(
				'SELECT * FROM devour_stats WHERE %s ORDER BY id DESC LIMIT 1',
				Run::WHERE_RUNNING
			))
			->fetch(PDO::FETCH_ASSOC)
		;

		return $row ? new Run($row) : NULL;
	}


	/**
	 *
	 */
	public function getContext(): ?string
	{
		$run = $this->getOpenRun();

		return $run ? $run->getContext() : NULL;
	}


	/**
	 *
	 */
	public function getSyncInterval($context = NULL, $mode = 'high'): ?int
	{
		$where = Run::WHERE_CONTEXT[(string) $context] ?? Run::WHERE_CONTEXT[''];

		//
		// The 90th percentile rather than the maximum.
		//
		// MAX let a single anomalous run set the ceiling permanently, and it counted runs that
		// barely started: a full sync that died after five seconds sat in the same history as the
		// two-hour ones.  A percentile absorbs both without needing a threshold for what counts as
		// a real run — measured against this data, filtering short runs out explicitly changed the
		// result by zero seconds.
		//
		$select = match ($mode) {
			'high'    => 'percentile_cont(0.9) WITHIN GROUP (ORDER BY seconds)',
			'average' => 'AVG(seconds)',
			default   => throw new \InvalidArgumentException(sprintf(
				'Unknown sync interval mode "%s".',
				$mode
			))
		};

		//
		// Bounded to the most recent runs so the estimate tracks how long syncs take now, rather
		// than being anchored to whatever the slowest run in all of history happened to be.
		//
		$result = $this->destination->query(sprintf(
			'SELECT %s AS result FROM (
				SELECT EXTRACT(EPOCH FROM (end_time - start_time)) AS seconds
				FROM devour_stats
				WHERE end_time IS NOT NULL AND %s
				ORDER BY start_time DESC
				LIMIT %d
			) recent',
			$select,
			$where,
			self::INTERVAL_SAMPLE
		))->fetch(PDO::FETCH_ASSOC);

		//
		// An aggregate over zero rows still returns one row holding NULL, so the rowCount() guard
		// this used to carry never fired — it returned seconds-since-midnight garbage instead.
		//
		if (!$result || $result['result'] === NULL) {
			return NULL;
		}

		return (int) $result['result'];
	}


	/**
	 *
	 */
	public function getCompletionTime($context = NULL): ?int
	{
		$run = $this->getOpenRun();

		if (!$run) {
			return NULL;
		}

		$interval = $this->getSyncInterval($context);

		//
		// No history for this context means no estimate.  Casting NULL to int here would return
		// the run's own start time, which reads as a real completion estimate that has already
		// passed rather than as "unknown".
		//
		if ($interval === NULL) {
			return NULL;
		}

		return strtotime($run->getStartTime()) + $interval;
	}

	/**
	 * 
	 */
	public function getStartTime(): ?int
	{
		$run = $this->getOpenRun();

		return $run ? strtotime($run->getStartTime()) : NULL;
	}


	/**
	 * 
	 */
	public function getScheduledBy(): ?string
	{
		$run = $this->getOpenRun();

		return $run ? $run->getScheduledBy() : NULL;
	}


	/**
	 * 
	 */
	public function getScheduledTime(): ?int
	{
		$result = $this->destination->query("
			SELECT
				scheduled_time
			FROM
				devour_stats
			WHERE
				end_time IS NULL
			AND
				start_time IS NULL
			LIMIT 1
		");

		if (!$result->rowCount()) {
			return NULL;
		}

		return strtotime($result->fetch(PDO::FETCH_ASSOC)['scheduled_time']);
	}


	/**
	 *
	 */
	public function getLastCompletedTime(): ?string
	{
		$result = $this->destination->query("
			SELECT
				end_time
			FROM
				devour_stats
			WHERE
				end_time IS NOT NULL
			ORDER BY
				start_time DESC
			LIMIT
				1
		");

		if (!$result->rowCount()) {
			return NULL;
		}

		return strtotime($result->fetch(PDO::FETCH_ASSOC)['end_time']);
	}


	/**
	 *
	 */
	public function getLastSyncTime(): ?string
	{
		$result = $this->destination->query("
			SELECT
				start_time
			FROM
				devour_stats
			WHERE
				end_time IS NOT NULL
			ORDER BY
				start_time DESC
			LIMIT
				1
		");

		if (!$result->rowCount()) {
			return NULL;
		}

		return strtotime($result->fetch(PDO::FETCH_ASSOC)['start_time']);
	}

	/**
	 * 
	 */
	public function getMapping($name)
	{
		return $this->mappings[$name] ?? NULL;
	}


	/**
	 * 
	 */
	public function getMappings(): array
	{
		return $this->mappings;
	}


	/**
	 * 
	 */
	public function getNextScheduled()
	{
		//
		// This read "WHRE" and so had never executed successfully.  The ordering was DESC too,
		// which would have returned the furthest-future row rather than the next one.
		//
		$result = $this->destination->query(
			'SELECT id FROM devour_stats
			 WHERE start_time IS NULL AND scheduled_time IS NOT NULL AND canceled_time IS NULL
			 ORDER BY scheduled_time ASC
			 LIMIT 1'
		)->fetch(PDO::FETCH_ASSOC);

		return $result ? (int) $result['id'] : NULL;
	}


	/**
	 *
	 */
	public function isScheduled(): ?bool
	{
		$result = $this->destination->query(
			'SELECT COUNT(*) AS scheduled FROM devour_stats
			 WHERE start_time IS NULL AND scheduled_time IS NOT NULL AND canceled_time IS NULL'
		)->fetch(PDO::FETCH_ASSOC);

		return (bool) $result['scheduled'];
	}


	/**
	 *
	 */
	public function isRunning(): ?bool
	{
		//
		// The rowCount() guard these methods used to carry was dead: an aggregate always returns
		// exactly one row, so the documented NULL return was unreachable.
		//
		$result = $this->destination->query(sprintf(
			'SELECT COUNT(*) AS running FROM devour_stats WHERE %s',
			Run::WHERE_RUNNING
		))->fetch(PDO::FETCH_ASSOC);

		return (bool) $result['running'];
	}


	/**
	 *
	 */
	public function schedule(array $mappings = array(), array $ids = array(), $scheduled_by = NULL, $force_update = FALSE): array
	{
		$this->stat();

		if (!$this->statGet('new')) {
			throw new RuntimeException(
				sprintf(
					'Syncing is already scheduled, scheduled at %s.',
					$this->statGet('scheduled_time')
				)
			);
		}

		$this->statSet('scheduled_by', $scheduled_by);
		$this->statSet('scheduled_time', date('Y-m-d H:i:s'));
		$this->statSet('tables', json_encode($mappings));
		$this->statSet('force', $force_update ? '1' : '0');
		if (count($mappings) == 1) {
			$this->statSet('ids', json_encode($ids));
		}

		return $this->stat;
	}


	/**
	 *
	 */
	public function run(array $mappings = array(), $ids = array(), $force_update = FALSE): array
	{
		$this->stat();

		if ($this->isRunning()) {
			throw new RuntimeException(
				sprintf(
					'Syncing is already running, started at %s.',
					$this->statGet('start_time')
				)
			);

		} else {
			if (!count($mappings) && ($tables = $this->statGet('tables'))) {
				$decoded = json_decode($tables, TRUE);

				if (is_array($decoded) && count($decoded)) {
					$mappings = $decoded;
				}
			}

			if (!count($ids) && ($stored_ids = $this->statGet('ids'))) {
				$decoded = json_decode($stored_ids, TRUE);

				if (is_array($decoded)) {
					$ids = $decoded;
				}
			}

			if (!$force_update && ($force = $this->statGet('force'))) {
				$force_update = (bool) $force;
			}

			if (!empty($this->pruneStatsWhere)) {
				$this->pruneStats();
			}
			$this->statSet('start_time', date('Y-m-d H:i:s'));
			$this->statSet('force', $force_update ? '1' : '0');

			if (function_exists('pcntl_signal')) {

				declare(ticks=1);

				$killer = function () {
					try {
						$this->statSet('end_time', date('Y-m-d H:i:s'));

					} catch (CanceledException $e) {
						//
						// Already cancelled — canceled_time is the terminator, and throwing out of
						// a signal handler would be a fatal rather than a clean exit.
						//
					}

					exit();
				};

				pcntl_signal(SIGINT,  $killer);
				pcntl_signal(SIGTERM, $killer);
			}

			if (!count($mappings)) {
				$mappings = array_keys($this->mappings);
			}

			try {
				foreach ($mappings as $mapping) {
					try {
						$this->syncMapping($mapping, $ids[$mapping] ?? [], $force_update);

					} catch (CanceledException $e) {
						//
						// Caught ahead of the generic handler below, which logs — and logging is
						// exactly what throws once the run has been cancelled.
						//
						throw $e;

					} catch (\Exception $e) {
						$this->logError($mapping, 'sync failed', $e->getMessage());
					}
				}

				$this->statSet('end_time', date('Y-m-d H:i:s'));

			} catch (CanceledException $e) {
				//
				// canceled_time is the terminator for a cancelled run.  Stamping end_time as well
				// would return it to the duration history it was deliberately excluded from.
				//
				return $this->stat;
			}
		}


		return $this->stat;
	}


	/**
	 *
	 */
	public function stat(): void
	{
		
		$result = $this->destination
			->query("SELECT * FROM devour_stats ORDER BY id DESC LIMIT 1")
			->fetch(PDO::FETCH_ASSOC)
		;
		if ($result && $result['scheduled_time'] && !$result['start_time']) {
			$this->stat = $result;
		} else {
			$this->stat = [
				'new'            => TRUE,
				'start_time'     => NULL,
				'scheduled_by'   => NULL,
				'scheduled_time' => NULL,
				'end_time'       => NULL,
				'canceled_time'  => NULL,
				'canceled_by'    => NULL,
				'heartbeat'      => NULL,
				'max_gap'        => NULL,
				'error'          => NULL,
				'table_stats'    => NULL,
				'tables'         => NULL,
				'ids'            => NULL,
				'log'            => NULL,
				'force'          => 0
			];
		}
	}


	/**
	 *
	 */
	public function statGet(string $column): ?string
	{
		if (!array_key_exists($column, $this->stat)) {
			return NULL;
		}

		return $this->stat[$column];
	}


	/**
	 *
	 */
	public function statSet(string $column, string $value): void
	{
		$this->stat[$column] = $value;
		if (is_bool($this->stat['force'])) {
			$this->stat['force'] = $this->stat['force'] ? '1' : '0';
		}

		//
		// Bound explicitly rather than passing $this->stat wholesale: the in-memory stat also
		// carries heartbeat, max_gap and id, and handing a statement parameters it does not
		// reference is an error.
		//
		$params = [];

		foreach (self::STAT_COLUMNS as $name) {
			$params[$name] = $this->stat[$name] ?? NULL;
		}

		if (array_key_exists('new', $this->stat)) {
			unset($this->stat['new']);

			$insert_statement = $this->destination->prepare(sprintf(
				'INSERT INTO devour_stats (%s) VALUES (%s)',
				join(', ', self::STAT_COLUMNS),
				':' . join(', :', self::STAT_COLUMNS)
			));

			$insert_statement->execute($params);

			$this->stat['id'] = $this->destination->lastInsertId();

		} else {
			$sets = array_map(function ($name) {
				return sprintf('%s = :%s', $name, $name);
			}, self::STAT_COLUMNS);

			$update_statement = $this->destination->prepare(sprintf(
				'UPDATE devour_stats SET %s WHERE id = :id AND canceled_time IS NULL',
				join(', ', $sets)
			));

			$update_statement->execute($params + ['id' => $this->stat['id']]);

			if ($update_statement->rowCount() < 1) {
				throw new CanceledException(sprintf(
					'Sync %s was cancelled.',
					$this->stat['id']
				));
			}
		}
	}


	/**
	 *
	 */
	public function updateGet(string $table): string
	{
		$time = '1800-01-01 00:00:00';

		$statement = $this->destination->prepare(
			'SELECT * FROM devour_updates WHERE target = :target LIMIT 1'
		);
		$statement->execute(['target' => $table]);

		$result = $statement->fetch(PDO::FETCH_ASSOC);

		if ($result) {
			return $result['time'];
		}

		$insert = $this->destination->prepare(
			'INSERT INTO devour_updates (target, time) VALUES (:target, :time)'
		);
		$insert->execute(['target' => $table, 'time' => $time]);

		return $time;
	}


	/**
	 *
	 */
	public function updateSet(string $table, string $time): void
	{
		//
		// $time is the moment the sync STARTED, not the moment it finished.  syncMapping() captures
		// it before doing any work so that records changed while the sync was running are picked up
		// by the next run rather than skipped.  This method used to discard $time and write "now"
		// instead, which advanced the watermark past those records permanently.
		//
		$statement = $this->destination->prepare(
			'UPDATE devour_updates SET time = :time WHERE target = :target'
		);

		$statement->execute(['time' => $time, 'target' => $table]);
	}

	/**
	 *
	 */
	protected function compare($a, $b)
	{
		return $a != $b ? ($a > $b ? 1 : -1) : 0;
	}


	/**
	 *
	 */
	protected function composeColumns($mapping, $row)
	{
		foreach ($mapping->getContextFields() as $alias => $field) {
			unset($row[$alias]);
		}

		return implode(', ', array_keys($row));
	}


	/**
	 *
	 */
	protected function composeParams($mapping, $row)
	{
		foreach ($mapping->getContextFields() as $alias => $field) {
			unset($row[$alias]);
		}

		return ':' . implode(', :', array_keys($row));
	}


	/**
	 *
	 */
	protected function composeSetParams($mapping, $row)
	{
		foreach ($mapping->getContextFields() as $alias => $field) {
			unset($row[$alias]);
		}

		$sets = array();

		foreach (array_keys($row) as $column) {
			$sets[] = sprintf("%s = :%s", $column, $column);
		}

		return implode(', ', $sets);
	}


	/**
	 *
	 */
	protected function filter(Mapping $mapping, array $row, $operation)
	{
		$data = array();

		foreach (array_keys($row) as $column) {
			$data[$column] = $row[$column];

			foreach ($mapping->getFilters($column) as $filter) {
				if (!isset($this->filters[$filter])) {
					throw new RuntimeException(sprintf(
						'Cannot filter column "%s" with "%s", filter not registered.',
						$column,
						$filter
					));
				}

				$data[$column] = $this->filters[$filter]($data[$column], $row, $operation);
			}

			if ($data[$column] instanceof DateTime) {
				$data[$column] = $data[$column]->format('Y-m-d H:i:s');
			}
		}

		return $data;
	}


	/**
	 *
	 */
	protected function filterKeys(Mapping $mapping, array $keys, $operation)
	{
		foreach ($keys as $i => $key) {
			$keys[$i] = $this->filter($mapping, $key, $operation);
		}

		return $keys;
	}


	/**
	 *
	 */
	protected function generate($alias, array $row)
	{
		if (isset($this->generators[$alias])) {
			return $this->generators[$alias]($row);
		}

		throw new RuntimeException(sprintf(
			'Cannot generate type "%s", generator not registered.',
			$alias
		));
	}


	/**
	 *
	 */
	protected function getExistingDestinationKeys(Mapping $mapping)
	{
		try {
			return $this->destination
				->query($mapping->composeDestinationExistingKeysQuery(), PDO::FETCH_ASSOC)
				->fetchAll()
			;

		} catch (\Exception $e) {
			$this->logError(
				$mapping->getDestination(),
				'existing destination key lookup failed',
				$e->getMessage()
			);
		}
	}


	/**
	 *
	 */
	protected function getExistingSourceKeys(Mapping $mapping)
	{
		try {
			return $this->source
				->query($mapping->composeSourceExistingKeysQuery(), PDO::FETCH_ASSOC)
				->fetchAll()
			;

		} catch (\Exception $e) {
			$this->logError(
				$mapping->getDestination(),
				'existing source key lookup failed',
				$e->getMessage()
			);
		}
	}


	/**
	 *
	 */
	protected function getUpdatedSourceKeys(Mapping $mapping, array $source_keys)
	{
		$updated_keys = array();

		foreach (array_chunk($source_keys, $this->chunkLimit) as $source_keys) {
			try {
				$updated_keys = array_merge(
					$updated_keys,
					$this->source
						->query($mapping->composeSourceUpdatedKeysQuery($source_keys), PDO::FETCH_ASSOC)
						->fetchAll()
				);

			} catch (\Exception $e) {
				$this->logError(
					$mapping->getDestination(),
					'updated source key lookup failed',
					$e->getMessage()
				);
			}

			sleep(static::SLEEP_TIME);
		}

		return $updated_keys;
	}


	/**
	 *
	 */
	protected function unsynced($name, $ids = null)
	{
		$synced = $this->synced[$name] ?? FALSE;

		if ($ids) {
			if (is_array($synced)) {
				$seen = [];

				foreach ($synced as $row) {
					$seen[$this->keySignature($name, $row)] = TRUE;
				}

				return array_filter($ids, function ($item) use ($name, $seen) {
					return !isset($seen[$this->keySignature($name, $item)]);
				});

			} elseif ($synced) {
				return FALSE;

			} else {
				return $ids;
			}
		}

		if ($synced) {
			return FALSE;
		}

		return TRUE;
	}


	/**
	 * A comparable signature for one key row of a mapping.
	 *
	 * Composite keys are the norm for adjunct tables — event_sessions keys on (code, event) — so
	 * this cannot assume a single column named 'id'.  It used to, which meant the second subset
	 * sync of any composite-key mapping within a run read an undefined 'id' offset and, on a host
	 * that promotes warnings to exceptions, aborted that mapping.
	 */
	protected function keySignature(string $name, array $row): string
	{
		$values = [];

		foreach ($this->mappings[$name]->getKey() as $field) {
			$values[] = (string) ($row[$field] ?? '');
		}

		return join("\0", $values);
	}


	/**
	 * Record what has been synced for a mapping.
	 *
	 * Subset results accumulate rather than replace: syncing two events in one run produces two
	 * batches of adjunct keys, and overwriting would let the first batch be synced again.
	 */
	protected function recordSynced($name, $ids): void
	{
		if (!$ids) {
			$this->synced[$name] = TRUE;

			return;
		}

		$existing = is_array($this->synced[$name] ?? NULL)
			? $this->synced[$name]
			: [];

		$this->synced[$name] = array_merge($existing, $ids);
	}


	/**
	 * 
	 */
	protected function isQueued($name, $ids = null)
	{
		return in_array($name, $this->stack);
	}


	/**
	 * 
	 */
	protected function queue($name, $ids = null)
	{
		array_push($this->stack, $name);
	}


	/**
	 *
	 */
	protected function now(): int
	{
		return time();
	}


	/**
	 * How much of a run's narration is kept in devour_stats.log.
	 *
	 * Defaults to SUMMARY.  The stored log is read long after the fact, through an admin screen or
	 * an email, where a line per table plus the errors is what anyone actually wants; step-by-step
	 * progress made a full sync's log tens of thousands of lines and buried the failures in them.
	 */
	public function setLogVerbosity(int $verbosity): void
	{
		$this->logVerbosity = $verbosity;
	}


	/**
	 * How much is echoed to stdout, which is separate and defaults to VERBOSE.
	 *
	 * Console output is ephemeral, so there is no reason to abbreviate it just because the stored
	 * log is abbreviated.
	 */
	public function setEchoVerbosity(int $verbosity): void
	{
		$this->echoVerbosity = $verbosity;
	}


	/**
	 *
	 */
	protected function log($message, int $level = self::VERBOSITY_VERBOSE)
	{
		$line = sprintf('[%s] %s', date('Y-m-d H:i:s', $this->now()), $message . PHP_EOL);

		if ($level <= $this->echoVerbosity) {
			echo $line;
		}

		//
		// logAppend() is still called when the line is not stored, because it carries the heartbeat
		// and the cancellation check.  A run whose verbosity hides every line must still be known
		// to be alive, and must still stop when cancelled.
		//
		$this->logAppend($line, $level <= $this->logVerbosity);
	}


	/**
	 * Record a failure against the mapping that produced it.
	 *
	 * Errors are aggregated by table and message rather than appended one per occurrence.  A real
	 * sync produces the same failure hundreds of times — one duplicate key repeated for every row
	 * of a batch — so a verbatim list is mostly repetition and is the largest thing in the log.
	 *
	 * $context is the offending row or query.  It is kept as a single example and only surfaces at
	 * DEBUG verbosity, because it is the bulk of an error line and is rarely the first thing needed.
	 */
	protected function logError(string $table, string $operation, string $message, $context = NULL): void
	{
		$message   = trim(preg_replace('/\s+/', ' ', $message));
		$signature = $table . "\0" . $operation . "\0" . $this->normalizeError($message);

		if (!isset($this->errors[$signature])) {
			$this->errors[$signature] = [
				'table'     => $table,
				'operation' => $operation,
				'message'   => $message,
				'sample'    => is_scalar($context) ? (string) $context : json_encode($context),
				'count'     => 0,
			];
		}

		$this->errors[$signature]['count']++;

		if (isset($this->tableStats[$table])) {
			$this->tableStats[$table]['failed']++;
		}

		$detail = $this->echoVerbosity >= self::VERBOSITY_DEBUG && $context !== NULL
			? sprintf(' -- %s', is_scalar($context) ? $context : json_encode($context))
			: '';

		$this->log(
			sprintf('[%s] %s: %s%s', $table, $operation, $message, $detail),
			self::VERBOSITY_ERRORS
		);

		$this->persistErrors();
	}


	/**
	 * Collapse the parts of a message that vary between otherwise identical failures.
	 *
	 * Quoted literals and bare numbers carry the offending value, so leaving them in would file
	 * every row of a failing batch as its own distinct error.
	 */
	protected function normalizeError(string $message): string
	{
		return preg_replace(['/\'[^\']*\'/', '/\b\d+\b/'], ["'?'", '#'], $message);
	}


	/**
	 * Render the aggregated errors, newest table last, capped so one pathological run cannot
	 * recreate in this column the size problem it was added to solve.
	 */
	public function composeErrors(): ?string
	{
		if (!$this->errors) {
			return NULL;
		}

		$lines = [];
		$shown = array_slice($this->errors, 0, self::ERROR_LIMIT);

		foreach ($shown as $error) {
			$lines[] = sprintf(
				'[%s] %s%s: %s%s',
				$error['table'],
				$error['count'] > 1 ? $error['count'] . ' x ' : '',
				$error['operation'],
				$error['message'],
				$error['sample'] !== NULL && $error['sample'] !== ''
					? sprintf(' (e.g. %s)', $error['sample'])
					: ''
			);
		}

		if (count($this->errors) > self::ERROR_LIMIT) {
			$lines[] = sprintf('... and %s more distinct errors', count($this->errors) - self::ERROR_LIMIT);
		}

		return join(PHP_EOL, $lines) . PHP_EOL;
	}


	/**
	 *
	 */
	/**
	 * Begin counting a table's work.
	 *
	 * These figures used to exist only as sentences in the log, which Analyzer read back by regular
	 * expression.  Counting them here means the log can be shortened without taking the statistics
	 * with it, and means a count is a number rather than something parsed out of prose.
	 */
	protected function openTableStats(string $table): void
	{
		$this->tableStats[$table] = [
			'start'       => date('Y-m-d H:i:s', $this->now()),
			'end'         => NULL,
			'duration'    => NULL,
			'transferred' => 0,
			'inserted'    => 0,
			'updated'     => 0,
			'deleted'     => 0,
			'failed'      => 0,
		];
	}


	/**
	 *
	 */
	protected function countTableStat(string $table, string $metric, int $amount): void
	{
		if (!isset($this->tableStats[$table])) {
			$this->openTableStats($table);
		}

		$this->tableStats[$table][$metric] += $amount;
	}


	/**
	 * Close a table's figures and emit its one-line summary.
	 */
	protected function closeTableStats(string $table): void
	{
		if (!isset($this->tableStats[$table])) {
			return;
		}

		$stats = &$this->tableStats[$table];

		$stats['end']      = date('Y-m-d H:i:s', $this->now());
		$stats['duration'] = $this->now() - strtotime($stats['start']);

		$parts = [];

		foreach (['transferred', 'inserted', 'updated', 'deleted', 'failed'] as $metric) {
			if ($stats[$metric]) {
				$parts[] = sprintf('%s %s', number_format($stats[$metric]), $metric);
			}
		}

		$this->log(
			sprintf(
				'[%s] %s in %s',
				$table,
				$parts ? join(', ', $parts) : 'nothing to do',
				$this->composeDuration($stats['duration'])
			),
			self::VERBOSITY_SUMMARY
		);

		$this->persistTableStats();
	}


	/**
	 *
	 */
	protected function composeDuration(int $seconds): string
	{
		if ($seconds < 60) {
			return $seconds . 's';
		}

		return sprintf('%dm%02ds', intdiv($seconds, 60), $seconds % 60);
	}


	/**
	 *
	 */
	protected function persistTableStats(): void
	{
		if (empty($this->stat['id'])) {
			return;
		}

		$this->stat['table_stats'] = json_encode($this->tableStats);

		$statement = $this->destination->prepare(
			'UPDATE devour_stats SET table_stats = :table_stats WHERE id = :id'
		);

		$statement->execute([
			'table_stats' => $this->stat['table_stats'],
			'id'          => $this->stat['id'],
		]);
	}


	/**
	 *
	 */
	protected function persistErrors(): void
	{
		if (empty($this->stat['id'])) {
			return;
		}

		$this->stat['error'] = $this->composeErrors();

		$statement = $this->destination->prepare(
			'UPDATE devour_stats SET error = :error WHERE id = :id'
		);

		$statement->execute([
			'error' => $this->stat['error'],
			'id'    => $this->stat['id'],
		]);
	}


	/**
	 * Append one line to the run's log, and stamp its liveness.
	 *
	 * The log is concatenated in the database rather than read, appended in PHP and written back.
	 * The old approach resent the entire accumulated log on every line, so write volume grew with
	 * the square of the log length.
	 *
	 * heartbeat and max_gap ride along on this UPDATE because it already runs on every line, which
	 * makes liveness tracking free.  max_gap only ever grows from an interval that was actually
	 * observed, so a run that dies contributes nothing to it.
	 */
	protected function logAppend(string $line, bool $store = TRUE): void
	{
		$now   = $this->now();
		$stamp = date('Y-m-d H:i:s', $now);

		if ($store) {
			$this->stat['log'] = ($this->stat['log'] ?? '') . $line;
		}

		if (empty($this->stat['id'])) {
			return;
		}

		$gap = NULL;

		if (!empty($this->stat['heartbeat'])) {
			$gap = $now - strtotime($this->stat['heartbeat']);
		}

		//
		// The running maximum is resolved here rather than in SQL: a CASE would need the gap bound
		// three times in one statement, and PDO's SQLite driver will not reuse a named parameter.
		// Only the run's own worker writes these columns, so there is nothing to race with.
		//
		$max = max((int) ($this->stat['max_gap'] ?? 0), $gap === NULL ? 0 : $gap);

		$statement = $this->destination->prepare(sprintf(
			"UPDATE devour_stats
			 SET %s
			     heartbeat = :heartbeat,
			     max_gap = :max_gap
			 WHERE id = :id AND canceled_time IS NULL",
			$store ? "log = COALESCE(log, '') || :line," : ''
		));

		$params = [
			'heartbeat' => $stamp,
			'max_gap'   => $max,
			'id'        => $this->stat['id'],
		];

		if ($store) {
			$params['line'] = $line;
		}

		$statement->execute($params);

		if ($statement->rowCount() < 1) {
			throw new CanceledException(sprintf(
				'Sync %s was cancelled.',
				$this->stat['id']
			));
		}

		$this->stat['heartbeat'] = $stamp;
		$this->stat['max_gap']   = $max;
	}


	/**
	 *
	 */
	protected function syncMapping($name, $ids, $force_update, $context = NULL)
	{
		if (!isset($this->mappings[$name])) {
			throw new RuntimeException(sprintf(
				'Cannot sync mapping "%s", no such mapping defined.',
				$name
			));
		}

		$mapping = $this->mappings[$name];

		//
		// Check if the current mapping is already synced
		//

		if (!($unsynced = $this->unsynced($name, $ids))) {
			return TRUE;
		}

		if (is_array($unsynced)) {
			$ids = $unsynced;
		}

		//
		// Check if the current mapping is already queued
		//

		if (!$ids && $this->isQueued($name)) {
			throw new RuntimeException(sprintf(
				'Cannot sync "%s", already queued for sync - check for circular dependency',
				$name
			));
		}

		$this->queue($name);

		//
		// Handle dependencies
		//


		foreach ($mapping->getDependencies() as $dependency) {

			// Backwards compatibility
			if (is_array($dependency)) {
				$dependency_table = $dependency['table'];
			} else {
				$dependency_table = $dependency;
			}

			// Sync everything

			if (empty($ids)) {
				$this->syncMapping($dependency_table, [], $force_update, Mapping::CONTEXT_DEPENDENCY);

			} else {
				$key_query = $mapping->composeSourceDependencyKeyQuery($this->mappings[$dependency_table], $ids);
				if ($key_query) {
					$keys = $this->source->query($key_query)->fetchAll();

					//
					// Don't need to sync if there's no keys
					//

					if ($keys) {
						$this->syncMapping($dependency_table, $keys, $force_update, Mapping::CONTEXT_DEPENDENCY);
					}

				// If there's no key mapping, just sync everything
				} else {
					$this->syncMapping($dependency_table, [], $force_update);
				}

			}
		}

		$this->beforeSyncMapping($mapping);

		if ($this->strictTime) {
			$mapping->addParam('lastSynced', $this->updateGet($name));
		} else {
			$mapping->addParam('lastSynced', date('Y-m-d', strtotime($this->updateGet($name))));
		}

		$mapping->addParam('today', date('Y-m-d'));

		$this->log(sprintf('Syncing %s', $name));

		$this->openTableStats($mapping->getDestination());

		$this->createTemporaryTable($mapping);
		$this->syncMappingTemporary($mapping, $ids);

		$start_sync_time  = date('Y-m-d H:i:s');

		if ($this->truncate[$mapping->getDestination()] && empty($ids)) {
			$this->truncateTable($mapping);
			$this->syncMappingInserts($mapping);

		} else {
			if ($mapping->canDelete()) {
				$this->syncMappingDeletes($mapping, $ids);
				$this->log('...completed deletions');
			}

			$this->syncMappingInserts($mapping);
			$this->log('...completed inserts');

			if ($mapping->canUpdate()) {
				$this->syncMappingUpdates($mapping, $force_update);
				$this->log('...completed updates');
			}
		}

		//
		// We use the start sync time, but set it after its completed in order to catch anything
		// that might be updated while the sync is taking place (in the next one)
		//

		$this->updateSet($name, $start_sync_time);

		$this->closeTableStats($mapping->getDestination());

		$this->recordSynced(array_pop($this->stack), $ids);

		//
		// This is exclusively for subsets, but we don't want to chain sub-items
		//
		if ($ids && !$context) {
			foreach ($mapping->getAdjuncts() as $adjunct => $config) {
				//
				// A site can disable or blacklist any mapping, so an adjunct naming one that was
				// not registered is a configuration outcome rather than a fault.  Skipped loudly:
				// unguarded, this was an undefined index.
				//
				if (!isset($this->mappings[$adjunct])) {
					$this->log(sprintf(
						'...skipping adjunct %s, no such mapping defined',
						$adjunct
					));

					continue;
				}

				$adjunct   = $this->mappings[$adjunct];
				$key_query = $mapping->composeSourceAdjunctKeyQuery($adjunct, $ids);
				$statement = $this->source->prepare($key_query);

				$statement->execute($mapping->composeSourceAdjunctKeyParams($ids));

				$keys = $this->filterKeys($adjunct, $statement->fetchAll(PDO::FETCH_ASSOC), 'select');

				if ($keys) {
					$this->syncMapping($adjunct->getDestination(), $keys, $force_update, Mapping::CONTEXT_ADJUNCT);
				}
			}
		}
	}


	/**
	 *
	 */
	protected function syncMappingDeletes(Mapping $mapping, $ids = array())
	{
		if (!$mapping->canDelete()) {
			return;
		}

		$delete_results = [];

		try {
			$delete_select_query = $mapping->composeSourceDeleteSelectQuery($ids);
			$delete_results      = $this->destination->query($delete_select_query)->fetchAll();
		} catch (\Exception $e) {
			$this->logError(
				$mapping->getDestination(),
				'delete selection failed',
				$e->getMessage(),
				$delete_select_query
			);
		}

		if (!count($delete_results)) {
			return NULL;
		} else {
			$this->log(sprintf('...deleting  %s records', count($delete_results)));
		}

		foreach ($delete_results as $deletion) {
			try {
				//
				// Bound rather than escaped: pg_escape_string() comes from ext-pgsql, a separate
				// extension from pdo_pgsql, and with no pgsql connection open it escapes without
				// the connection's encoding context.
				//
				$key_wheres = [];
				$key_values = [];

				foreach (array_values($mapping->getKey()) as $i => $key) {
					$key_wheres[]           = sprintf('%s = :key%d', $key, $i);
					$key_values['key' . $i] = $deletion[$key];
				}

				$destination_delete_query = sprintf(
					'DELETE FROM %s WHERE %s',
					$mapping->getDestination(),
					join(' AND ', $key_wheres)
				);

				$statement = $this->destination->prepare($destination_delete_query);
				$statement->execute($key_values);

				$this->countTableStat($mapping->getDestination(), 'deleted', 1);

			} catch (\Exception $e) {
				$this->logError(
					$mapping->getDestination(),
					'delete failed',
					$e->getMessage(),
					$destination_delete_query
				);
			}
		}
	}


	/**
	 *
	 */
	protected function syncMappingInserts(Mapping $mapping)
	{
		if (!$mapping->canInsert()) {
			return;
		}

		$insert_results = array();
		$generated      = $mapping->getGenerators();

		try {
			$source_select_query = $mapping->composeSourceInsertSelectQuery();
			$insert_results      = $this->destination->query($source_select_query, PDO::FETCH_ASSOC)->fetchAll();
		} catch (\Exception $e) {
			$this->logError(
				$mapping->getDestination(),
				'insert selection failed',
				$e->getMessage(),
				$source_select_query
			);
		}

		if (!count($insert_results)) {
			return NULL;
		} else {
			$this->log(sprintf('...inserting  %s records', count($insert_results)));
		}

		foreach ($insert_results as $i => $row) {
			if (!$i) {
				$full_row = $row + $generated;
				$insert_statement = $this->destination->prepare(sprintf(
					'INSERT INTO %s (%s) VALUES(%s)',
					$mapping->getDestination(),
					$this->composeColumns($mapping, $full_row),
					$this->composeParams($mapping, $full_row)
				));
			}

			foreach ($this->filter($mapping, $row, 'INSERT') as $column => $value) {
				if (in_array($column, array_keys($mapping->getContextFields()))) {
					continue;
				}

				$insert_statement->bindValue(':' . $column, $value, $this->getPdoType($value));
			}

			foreach ($mapping->getGenerators() as $column => $generator) {
				$value = $this->generate($generator, $row);
				$insert_statement->bindValue(':' . $column, $value, $this->getPdoType($value));
			}

			try {
				$insert_statement->execute();

				$this->countTableStat($mapping->getDestination(), 'inserted', 1);

			} catch (\Exception $e) {
				$this->logError(
					$mapping->getDestination(),
					'insert failed',
					$e->getMessage(),
					json_encode($this->filter($mapping, $row, 'INSERT'))
				);
			}
		}
	}


	/**
	 *
	 */
	protected function syncMappingTemporary(Mapping $mapping, $ids = array())
	{
		$source_select_query = $mapping->composeSourceSelectQuery($ids);
		$query_database      = $this->getTransferSelectDatabase($mapping);
		$query_location      = $this->getTransferSelectDatabaseName($mapping);
		try {
			$source_results = $query_database->query($source_select_query, PDO::FETCH_ASSOC)->fetchAll();
			$generated      = array_keys($mapping->getGenerators());

			$this->log(sprintf('...transfering %s temporary records', count($source_results)));

			foreach ($source_results as $i => $row) {
				if (!$i) {
					$full_row = $row + array_flip($generated);
					$insert_statement = $this->destination->prepare(sprintf(
						'INSERT INTO devour_temp_%s (%s) VALUES(%s)',
						$mapping->getDestination(),
						$this->composeColumns($mapping, $full_row),
						$this->composeParams($mapping, $full_row)
					));
				}

				foreach ($this->filter($mapping, $row, 'INSERT') as $column => $value) {
					if (in_array($column, array_keys($mapping->getContextFields()))) {
						continue;
					}

					$insert_statement->bindValue(':' . $column, $value, $this->getPdoType($value));
				}

				foreach ($mapping->getGenerators() as $column => $generator) {
					$value = $this->generate($generator, $row);
					$insert_statement->bindValue(':' . $column, $value, $this->getPdoType($value));
				}

				try {
					$insert_statement->execute();

					$this->countTableStat($mapping->getDestination(), 'transferred', 1);

				} catch (\Exception $e) {
					$this->logError(
						$mapping->getDestination(),
						'transfer failed',
						$e->getMessage(),
						json_encode($this->filter($mapping, $row, 'INSERT'))
					);
				}
			}
		} catch (\Exception $e) {
			$this->logError(
				$mapping->getDestination(),
				sprintf('transfer selection from %s failed', $query_location),
				$e->getMessage(),
				$source_select_query
			);
		}
	}

	/**
	 *
	 */
	protected function syncMappingUpdates(Mapping $mapping, $force = FALSE)
	{
		if (!$mapping->canUpdate()) {
			return;
		}

		$offset = 0;
		do {
			try {
				$source_select_query = $mapping->composeSourceUpdateSelectQuery($force, $this->chunkLimit, $offset);
				$update_results      = $this->destination->query($source_select_query, PDO::FETCH_ASSOC)->fetchAll();
			} catch (\Exception $e) {
				$this->logError(
					$mapping->getDestination(),
					'update selection failed',
					$e->getMessage(),
					$source_select_query
				);
			}

			if (!$force) {
				$this->log('...gathering updated records');
			}

			if (!count($update_results)) {
				return NULL;
			} else {
				$this->log(sprintf('...updating  %s records', count($update_results)));
			}
			
			foreach ($update_results as $i => $row) {
				if (!$i) {
					$update_statement = $this->destination->prepare(sprintf(
						'UPDATE %s SET %s WHERE %s',
						$mapping->getDestination(),
						$this->composeSetParams($mapping, $row),
						join(' AND ', array_map(function($field) {
							return sprintf('%s = :__%s', $field, $field);
						}, $mapping->getKey()))
					));
				}

				foreach ($row as $column => $value) {
					if (in_array($column, array_keys($mapping->getContextFields()))) {
						continue;
					}

					$index = array_search($column, $mapping->getKey());

					$update_statement->bindValue(':' . $column, $value, $this->getPdoType($value));

					if ($index !== FALSE) {
						$update_statement->bindValue(':__' . $column, $value, $this->getPdoType($value));
					}
				}
				

				try {
					$update_statement->execute();

					$this->countTableStat($mapping->getDestination(), 'updated', 1);

				} catch (\Exception $e) {
					$this->logError(
						$mapping->getDestination(),
						'update failed',
						$e->getMessage(),
						json_encode($this->filter($mapping, $row, 'UPDATE'))
					);
				}
			}

			$offset += $this->chunkLimit;
		} while ($mapping->isChunked() && count($update_results) >= $this->chunkLimit);
	}


	/**
	 * 
	 */
	protected function truncateTable(Mapping $mapping)
	{
		try {
			//
			// This used to pass the table to query() as a second argument, where PDO expects a
			// fetch mode — so the literal string "TRUNCATE TABLE %s" was executed every time, threw,
			// and was swallowed by the catch below.
			//
			$this->destination->exec(sprintf(
				'TRUNCATE TABLE %s',
				$mapping->getDestination()
			));

		}  catch (\Exception $e) {
			$this->logError(
				$mapping->getDestination(),
				'truncate failed',
				$e->getMessage()
			);
		}
	}


	/**
	 *
	 */
	private function getPdoType($value)
	{
		if (is_int($value)) {
			return PDO::PARAM_INT;
		} elseif (is_bool($value)) {
			return PDO::PARAM_BOOL;
		} elseif (is_null($value)) {
			return PDO::PARAM_NULL;
		} else {
			return PDO::PARAM_STR;
		}
	}


	/**
	 *
	 */
	protected function beforeSyncMapping(Mapping $mapping)
	{
		return;
	}


	/**
	 *
	 */
	protected function getTransferSelectDatabase(Mapping $mapping)
	{
		return $this->source;
	}


	/**
	 *
	 */
	protected function getTransferSelectDatabaseName(Mapping $mapping)
	{
		return 'source';
	}


	/**
	 * 
	 */
	private function pruneStats()
	{
		$pruneCriteria = implode(' AND ', $this->pruneStatsWhere);

		$this->destination->query("
			DELETE FROM devour_stats
			WHERE $pruneCriteria
		");
	}

	
	/**
	 * 
	 */
	public function setPruneStatsWhere($pruneStatsWhere = [])
	{
		$this->pruneStatsWhere = $pruneStatsWhere;
	}
}
