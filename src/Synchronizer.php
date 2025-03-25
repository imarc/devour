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
	public function addGenerator($name, callable $generator)
	{
		$this->generators[$name] = $generator;
	}


	/**
	 *
	 */
	public function createStatsTable()
	{
		$this->destination->query("
			CREATE TABLE devour_stats(
				id INT AUTO_INCREMENT PRIMARY KEY,
				start_time TIMESTAMP,
				scheduled_time TIMESTAMP,
				end_time TIMESTAMP,
				tables TEXT,
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
	public function getStartTime(): ?int
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

		return strtotime($result->fetch(PDO::FETCH_ASSOC)['start_time']);
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
	public function isScheduled(): ?bool
	{
		$result = $this->destination->query("
			SELECT
				COUNT(*) as running
			FROM
				devour_stats
			WHERE
				start_time IS NULL
			AND
				scheduled_time IS NOT NULL
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
	public function isRunning(): ?bool
	{
		$result = $this->destination->query("
			SELECT
				COUNT(*) as running
			FROM
				devour_stats
			WHERE
				end_time IS NULL
			AND
				start_time IS NOT NULL
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
	public function schedule(array $mappings = array()): array
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

		$this->statSet('scheduled_time', date('Y-m-d H:i:s'));
		$this->statSet('tables', implode(', ', $mappings));

		return $this->stat;
	}


	/**
	 *
	 */
	public function run(array $mappings = array(), $force_update = FALSE): array
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
			if (!empty($this->pruneStatsWhere)) {
				$this->pruneStats();
			}
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
		if (!$this->isScheduled() && !$this->isRunning()) {
			$this->stat = $this->destination
				->query("SELECT * FROM devour_stats ORDER BY id DESC LIMIT 1")
				->fetch(PDO::FETCH_ASSOC)
			;
		} else {
			$this->stat = [
				'new'            => TRUE,
				'start_time'     => NULL,
				'scheduled_time' => NULL,
				'end_time'       => NULL,
				'tables'         => NULL,
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

		if (array_key_exists('new', $this->stat)) {
			unset($this->stat['new']);

			$insert_statement  = $this->destination->prepare(
				"INSERT INTO devour_stats VALUES(:start_time, :scheduled_time, :end_time, :tables, :force, :log)"
			);

			$insert_statement->execute($this->stat);

			$this->stat['id'] = $this->destination->lastInsertId();

		} else {
			$update_statement = $this->destination->prepare(
				"UPDATE devour_stats SET start_time = :start_time, scheduled_time = :scheduled_time, end_time = :end_time, tables = :tables, log = :log, force = :force WHERE id = :id"
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

		$this->createTemporaryTable($mapping);
		$this->syncMappingTemporary($mapping);

		$start_sync_time  = date('Y-m-d H:i:s');

		if ($this->truncate[$mapping->getDestination()]) {
			$this->truncateTable($mapping);
			$this->syncMappingInserts($mapping);

		} else {
			if ($mapping->canDelete()) {
				$this->syncMappingDeletes($mapping);
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

		$this->synced[array_pop($this->stack)] = TRUE;
	}


	/**
	 *
	 */
	protected function syncMappingDeletes(Mapping $mapping)
	{
		if (!$mapping->canDelete()) {
			return;
		}

		try {
			$delete_select_query = $mapping->composeSourceDeleteSelectQuery();
			$delete_results      = $this->destination->query($delete_select_query)->fetchAll();
		} catch (\Exception $e) {
			$this->log(sprintf(
				"Failed selecting delete results with query: %s  The database returned: %s",
				$delete_select_query,
				$e->getMessage()
			));
		}

		if (!count($delete_results)) {
			return NULL;
		} else {
			$this->log(sprintf('...deleting  %s records', count($delete_results)));
		}

		foreach ($delete_results as $deletion) {
			try {
				$key_wheres = [];
				foreach ($mapping->getKey() as $key) {
					$key_wheres[] = sprintf("%s = '%s'", $key,  $deletion[$key]);
				}

				$this->destination->query(sprintf(
					'DELETE FROM %s WHERE %s',
					$mapping->getDestination(),
					join(' AND ', $key_wheres)
				), PDO::FETCH_ASSOC);
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
			$this->log(sprintf(
				"Failed selecting insert results with query: %s  The database returned: %s",
				$source_select_query,
				$e->getMessage()
			));
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
			} catch (\Exception $e) {
				$this->log(sprintf(
					'Failed inserting into %s with the following: %s  The database returned: %s',
					ucwords(str_replace('_', ' ', $mapping->getDestination())),
					json_encode($this->filter($mapping, $row, 'INSERT')),
					$e->getMessage()
				));
			}
		}
	}


	/**
	 *
	 */
	protected function syncMappingTemporary(Mapping $mapping)
	{
		$source_select_query = $mapping->composeSourceSelectQuery();
		try {
			$source_results = $this->source->query($source_select_query, PDO::FETCH_ASSOC)->fetchAll();
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
				} catch (\Exception $e) {
					$this->log(sprintf(
						'Failed inserting into %s with the following: %s  The database returned: %s',
						ucwords(str_replace('_', ' ', $mapping->getDestination())),
						json_encode($this->filter($mapping, $row, 'INSERT')),
						$e->getMessage()
					));
				}
			}
		} catch (\Exception $e) {
			$this->log(sprintf(
				"Failed selecting transfer results with query: %s  The database returned: %s",
				$source_select_query,
				$e->getMessage()
			));
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
				$this->log(sprintf(
					"Failed selecting update results with query: %s  The database returned: %s",
					$source_select_query,
					$e->getMessage()
				));
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

				} catch (\Exception $e) {
					$this->log(sprintf(
						'Failed updating %s with the following: %s  The database returned: %s',
						ucwords(str_replace('_', ' ', $mapping->getDestination())),
						json_encode($this->filter($mapping, $row, 'UPDATE')),
						$e->getMessage()
					));
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
			$this->destination->query('TRUNCATE TABLE %s', $mapping->getDestination());
		}  catch (\Exception $e) {
			$this->log(sprintf('Could not truncate destination table: %s  The database returned: %s',
				$mapping->getDestination(),
				$e->getMessage()
			));
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
