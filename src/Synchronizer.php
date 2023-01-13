<?php

namespace Devour;

use PDO;
use DateTime;
use PDOException;
use RuntimeException;

/**
 *
 */
class Synchronizer
{
	const SLEEP_TIME  = 0;

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
	public function __construct(PDO $source, PDO $destination, $strict_time = FALSE, $chunk_limit = 5000)
	{
		$this->source      = $source;
		$this->destination = $destination;
		$this->strictTime  = $strict_time;
		$this->chunkLimit  = $chunk_limit;

		if (!$this->hasStatsTable()) {
			$this->createStatsTable();
		}

		if (!$this->hasUpdatesTable()) {
			$this->createUpdatesTable();
		}
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
	public function createStatsTable()
	{
		$this->destination->query("
			CREATE TABLE devour_stats(
				start_time TIMESTAMP PRIMARY KEY,
				end_time TIMESTAMP,
				force BOOLEAN,
				log TEXT
			);
		");
	}


	/**
	 *
	 */
	public function createUpdatesTable()
	{
		$this->destination->query("
			CREATE TABLE devour_updates(
				target VARCHAR(255) PRIMARY KEY,
				time TIMESTAMP
			);
		");
	}


	/**
	 *
	 */
	public function getHighSyncInterval(): ?int
	{
		$result = $this->destination->query("
			SELECT
				MAX(end_time - start_time) as interval
			FROM
				devour_stats
		");

		if (!$result->rowCount()) {
			return NULL;
		}

		return strtotime($result->fetch(PDO::FETCH_ASSOC)['interval']) - strtotime('00:00:00');
	}


	/**
	 *
	 */
	public function getCompletionTime(): ?int
	{
		$result = $this->destination->query("
			SELECT
				start_time
			FROM
				devour_stats
			WHERE
				end_time IS NULL
			LIMIT 1
		");

		if (!$result->rowCount()) {
			return NULL;
		}

		return strtotime($result->fetch(PDO::FETCH_ASSOC)['start_time']) + $this->getHighSyncInterval();
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
	public function isRunning(): bool
	{
		$result = $this->destination->query("
			SELECT
				COUNT(*) as running
			FROM
				devour_stats
			WHERE
				end_time IS NULL
			LIMIT
				1
		");

		if (!$result->rowCount()) {
			return NULL;
		}

		return (bool) $result->fetch(PDO::FETCH_ASSOC)['running'];
	}



	/**
	 *
	 */
	public function run(array $mappings = array(), $force_update = FALSE): array
	{
		$this->stat();

		if (!$this->statGet('new')) {
			throw new RuntimeException(
				sprintf(
					'Syncing is already running, started at %s.',
					$this->statGet('start_time')
				)
			);

		} else {
			$this->statSet('start_time', date('Y-m-d H:i:s'));
			$this->statSet('force', $force_update ? 1 : 0);

			if (function_exists('pcntl_signal')) {

				declare(ticks=1);

				$killer = function () {
					$this->statSet('end_time', date('Y-m-d H:i:s'));
					exit();
				};

				pcntl_signal(SIGINT,  $killer);
				pcntl_signal(SIGTERM, $killer);
			}

			if (!count($mappings)) {
				$mappings = array_keys($this->mappings);
			}

			foreach ($mappings as $mapping) {
				try {
					$this->syncMapping($mapping, $force_update);

				} catch (\Exception $e) {
					$this->log($e->getMessage());
				}
			}

			$this->statSet('end_time', date('Y-m-d H:i:s'));
		}


		return $this->stat;
	}


	/**
	 *
	 */
	public function stat(): void
	{
		$result = $this->destination
			->query("SELECT * FROM devour_stats ORDER BY start_time DESC LIMIT 1")
			->fetch(PDO::FETCH_ASSOC)
		;

		if ($result && !$result['end_time']) {
			$this->stat = $result;

		} else {
			$this->stat = [
				'new'        => TRUE,
				'start_time' => NULL,
				'end_time'   => NULL,
				'log'        => NULL,
				'force'      => 0
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

		if (array_key_exists('new', $this->stat)) {
			unset($this->stat['new']);

			$insert_statement  = $this->destination->prepare(
				"INSERT INTO devour_stats VALUES(:start_time, :end_time, :log, :force)"
			);

			$insert_statement->execute($this->stat);

		} else {
			$update_statement = $this->destination->prepare(
				"UPDATE devour_stats SET end_time = :end_time, log = :log, force = :force WHERE start_time = :start_time"
			);

			$update_statement->execute($this->stat);
		}
	}


	/**
	 *
	 */
	public function updateGet(string $table): string
	{
		$time   = '1800-01-01 00:00:00';
		$result = $this->destination
			->query(sprintf(
				"SELECT * FROM devour_updates WHERE target = '%s' LIMIT 1",
				$table
			))
			->fetch(PDO::FETCH_ASSOC)
		;

		if ($result) {
			$time = $result['time'];

		} else {
			$this->destination->query(sprintf(
				"INSERT INTO devour_updates (target, time) VALUES('%s', '%s')",
				$table,
				$time
			));
		}

		return $time;
	}


	/**
	 *
	 */
	public function updateSet(string $table, string $time): void
	{
		$this->destination->query(sprintf(
			"UPDATE devour_updates SET TIME = '%s' WHERE target ='%s'",
			date("Y-m-d H:i:s"),
			$table
		));
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
	protected function composeColumns($row)
	{
		return implode(', ', array_keys($row));
	}


	/**
	 *
	 */
	protected function composeParams($row)
	{
		return ':' . implode(', :', array_keys($row));
	}


	/**
	 *
	 */
	protected function composeSetParams($row)
	{
		$sets = array();

		foreach (array_keys($row) as $column) {
			$sets[] = sprintf("%s = :%s", $column, $column);
		}

		return implode(', ', $sets);
	}


	/**
	 *
	 */
	protected function filter(Mapping $mapping, array $row)
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

				$data[$column] = $this->filters[$filter]($data[$column], $row);
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
	protected function filterKeys(Mapping $mapping, array $keys)
	{
		foreach ($keys as $i => $key) {
			$keys[$i] = $this->filter($mapping, $key);
		}

		return $keys;
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
			$this->log($e->getMessage());
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
			$this->log($e->getMessage());
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
				$this->log($e->getMessage());
			}

			sleep(static::SLEEP_TIME);
		}

		return $updated_keys;
	}


	/**
	 *
	 */
	protected function hasStatsTable()
	{
		$this->destination->query("SELECT 1");

		try {
			$this->destination->query("SELECT 1 FROM devour_stats");

			return TRUE;

		} catch (PDOException $e) {
			return FALSE;

		}
	}


	/**
	 *
	 */
	protected function hasUpdatesTable()
	{
		$this->destination->query("SELECT 1");

		try {
			$this->destination->query("SELECT 1 FROM devour_updates");

			return TRUE;

		} catch (PDOException $e) {
			return FALSE;

		}
	}


	/**
	 *
	 */
	protected function log($message)
	{
		$line = sprintf('[%s] %s', date('h:i:s'), $message . PHP_EOL);

		echo $line;
		$this->statSet('log', $this->statGet('log') . $line);
	}


	/**
	 *
	 */
	protected function syncMapping($name, $force_update)
	{
		if (!isset($this->mappings[$name])) {
			throw new RuntimeException(sprintf(
				'Cannot sync mapping "%s", no such mapping defined.',
				$name
			));
		}

		$mapping = $this->mappings[$name];

		if ($this->synced[$name]) {
			return TRUE;
		}

		if (in_array($name, $this->stack)) {
			throw new RuntimeException(sprintf(
				'Cannot sync "%s", already queued for sync - check for circular dependency',
				$name
			));
		}

		array_push($this->stack, $name);

		//

		foreach ($mapping->getDependencies() as $dependency) {
			$this->syncMapping($dependency, $force_update);
		}

		if ($this->strictTime) {
			$mapping->addParam('lastSynced', $this->updateGet($name));
		} else {
			$mapping->addParam('lastSynced', date('Y-m-d', strtotime($this->updateGet($name))));
		}

		$mapping->addParam('today', date('Y-m-d'));

		$this->log(sprintf('Syncing %s', $name));

		$source_keys      = $this->getExistingSourceKeys($mapping);
		$destination_keys = $this->getExistingDestinationKeys($mapping);
		$start_sync_time  = date('Y-m-d H:i:s');

		if (is_null($source_keys)) {
			throw new RuntimeException('Failed to acquire source keys, cannot continue.');
		}

		if (is_null($destination_keys)) {
			throw new RuntimeException('Failed to acquire destination keys, cannot continue.');
		}

		if ($this->truncate[$mapping->getDestination()]) {
			$this->syncMappingDeletes($mapping, array(), $destination_keys);
			$this->syncMappingInserts($mapping, $source_keys, array());

		} else {
			$this->syncMappingDeletes($mapping, $source_keys, $destination_keys);
			$this->log('...completed deletions');

			$this->syncMappingInserts($mapping, $source_keys, $destination_keys);
			$this->log('...completed inserts');

			if ($force_update) {
				$this->syncMappingUpdates($mapping, $source_keys, $destination_keys, TRUE);
			} else {
				$this->syncMappingUpdates($mapping, $source_keys, $destination_keys);
			}
			$this->log('...completed updates');
		}

		//
		// We use the start sync time, but set it after its completed in order to catch anything
		// that might be updated while the sync is taking place (in the next one)
		//

		$this->updateSet($name, $start_sync_time);

		$this->synced[array_pop($this->stack)] = TRUE;
	}


	/**
	 *
	 */
	protected function syncMappingDeletes(Mapping $mapping, array $source_keys, array $destination_keys)
	{
		$destination_keys = array_udiff(
			$destination_keys,
			$this->filterKeys($mapping, $source_keys),
			[$this, 'compare']
		);

		if (!count($destination_keys)) {
			return NULL;
		} else {
			$this->log(sprintf('...deleting  %s records', count($destination_keys)));
		}

		foreach (array_chunk($destination_keys, $this->chunkLimit) as $destination_keys) {
			$destination_delete_query = $mapping->composeDestinationDeleteQuery($destination_keys);

			try {
				$delete_results = $this->destination->query($destination_delete_query);
			} catch (\Exception $e) {
				$this->log(sprintf(
					"Failed removing destination records with query: %s  The database returned: %s",
					$destination_delete_query,
					$e->getMessage()
				));
			}
		}
	}


	/**
	 *
	 */
	protected function syncMappingInserts(Mapping $mapping, array $source_keys, array $destination_keys)
	{
		$diffed_keys = array_keys(array_udiff(
			$this->filterKeys($mapping, $source_keys),
			$destination_keys,
			[$this, 'compare']
		));

		if (!count($diffed_keys)) {
			return NULL;
		} else {
			$this->log(sprintf('...inserting  %s records', count($diffed_keys)));
		}

		$source_keys = array_filter($source_keys, function($key) use ($diffed_keys) {
			return in_array($key, $diffed_keys);
		}, ARRAY_FILTER_USE_KEY);

		foreach (array_chunk($source_keys, $this->chunkLimit) as $source_keys) {
			$insert_results      = array();
			$source_select_query = $mapping->composeSourceSelectQuery($source_keys);

			try {
				$insert_results = $this->source->query($source_select_query, PDO::FETCH_ASSOC);
			} catch (\Exception $e) {
				$this->log(sprintf(
					"Failed selecting insert results with query: %s  The database returned: %s",
					$source_select_query,
					$e->getMessage()
				));
			}

			foreach ($insert_results as $i => $row) {
				if (!$i) {
					$insert_statement = $this->destination->prepare(sprintf(
						'INSERT INTO %s (%s) VALUES(%s)',
						$mapping->getDestination(),
						$this->composeColumns($row),
						$this->composeParams($row)
					));
				}

				foreach ($this->filter($mapping, $row) as $column => $value) {
					$insert_statement->bindValue(':' . $column, $value, $this->getPdoType($value));
				}

				try {
					$insert_statement->execute();
				} catch (\Exception $e) {
					$this->log(sprintf(
						'Failed inserting into %s with the following: %s  The database returned: %s',
						ucwords(str_replace('_', ' ', $mapping->getDestination())),
						json_encode($this->filter($mapping, $row)),
						$e->getMessage()
					));
				}
			}
		}
	}


	/**
	 *
	 */
	protected function syncMappingUpdates(Mapping $mapping, array $source_keys, array $destination_keys, $force = FALSE)
	{
		if (!$force) {
			//
			// If we're not forcing updates, we get a list of only updated keys before filtering
			// and modifying them.
			//

			$this->log('...gathering updated records');
			$source_keys = $this->getUpdatedSourceKeys($mapping, $source_keys);

		}

		$intersect_keys = array_keys(array_uintersect(
			$this->filterKeys($mapping, $source_keys),
			$destination_keys,
			[$this, 'compare']
		));

		if (!count($intersect_keys)) {
			return NULL;
		} else {
			$this->log(sprintf('...updating  %s records', count($intersect_keys)));
		}

		$source_keys = array_filter($source_keys, function($key) use ($intersect_keys) {
			return in_array($key, $intersect_keys);
		}, ARRAY_FILTER_USE_KEY);

		foreach (array_chunk($source_keys, $this->chunkLimit) as $source_keys) {
			$update_results      = array();
			$source_select_query = $mapping->composeSourceSelectQuery($source_keys);

			try {
				$update_results = $this->source->query($source_select_query, PDO::FETCH_ASSOC);
			} catch (\Exception $e) {
				$this->log(sprintf(
					"Failed selecting update results with query: %s  The database returned: %s",
					$source_select_query,
					$e->getMessage()
				));
			}

			foreach ($update_results as $i => $row) {
				if (!$i) {
					$update_statement = $this->destination->prepare(sprintf(
						'UPDATE %s SET %s WHERE %s',
						$mapping->getDestination(),
						$this->composeSetParams($row),
						join(' AND ', array_map(function($field) {
							return sprintf('%s = :__%s', $field, $field);
						}, $mapping->getKey()))
					));
				}

				foreach ($this->filter($mapping, $row) as $column => $value) {
					$index = array_search($column, $mapping->getKey());

					$update_statement->bindValue(':' . $column, $value, $this->getPdoType($value));

					if ($index !== FALSE) {
						$update_statement->bindValue(':__' . $column, $value, $this->getPdoType($value));
					}
				}

				try {
					$update_statement->execute();

				} catch (\Exception $e) {
					$this->log(sprintf(
						'Failed updating %s with the following: %s  The database returned: %s',
						ucwords(str_replace('_', ' ', $mapping->getDestination())),
						json_encode($this->filter($mapping, $row)),
						$e->getMessage()
					));
				}
			}
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
}
