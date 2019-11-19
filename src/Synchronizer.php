<?php

namespace Devour;

use PDO;
use DateTime;
use RuntimeException;

/**
 *
 */
class Synchronizer
{
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
	protected $synced = array();


	/**
	 *
	 */
	protected $mappings = array();


	/**
	 *
	 */
	protected $truncate = array();


	/**
	 *
	 */
	public function __construct(PDO $source, PDO $destination)
	{
		$this->source      = $source;
		$this->destination = $destination;
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
	public function run(array $mappings = array(), $force_update = FALSE)
	{
		if (!count($mappings)) {
			$mappings = array_keys($this->mappings);
		}

		foreach ($mappings as $mapping) {
			if (!isset($this->mappings[$mapping])) {
				throw new RuntimeException(sprintf(
					'Cannot sync mapping "%s", no such mapping defined.',
					$mapping
				));
			}

			$this->syncMapping($mapping, $force_update);
		}
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
			echo $e->getMessage();
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
			echo $e->getMessage();
		}
	}


	/**
	 *
	 */
	protected function getUpdatedSourceKeys(Mapping $mapping, array $source_keys)
	{
		if (!$source_keys) {
			return array();
		}

		foreach (array_chunk($source_keys, 1000) as $source_keys) {
			try {
				return $this->source
					->query($mapping->composeSourceUpdatedKeysQuery($source_keys))
					->fetchAll()
				;

			} catch (\Exception $e) {
				echo $e->getMessage();
			}
		}
	}


	/**
	 *
	 */
	protected function syncMapping($name, $force_update)
	{
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
		// TODO: Get last synchronizer run on this mapping and set last_synced, for now fake:
		//

		$mapping->addParam('lastSynced', date('Y-m-d'));

		//

		foreach ($mapping->getDependencies() as $dependency) {
			$this->syncMapping($dependency, $force_update);
		}

		$source_keys      = $this->getExistingSourceKeys($mapping);
		$destination_keys = $this->getExistingDestinationKeys($mapping);

		echo sprintf('Syncing %s' . PHP_EOL, $name);

		if ($this->truncate[$mapping->getDestination()]) {
			$this->syncMappingDeletes($mapping, array(), $destination_keys);
			$this->syncMappingInserts($mapping, $source_keys, array());

		} else {
			$this->syncMappingDeletes($mapping, $source_keys, $destination_keys);
			$this->syncMappingInserts($mapping, $source_keys, $destination_keys);

			if ($force_update) {
				$this->syncMappingUpdates($mapping, $source_keys, $destination_keys);
			} else {
				$this->syncMappingUpdates($mapping, $source_keys, array());
			}

		}

		$this->synced[array_pop($this->stack)] = TRUE;
	}


	/**
	 *
	 */
	protected function syncMappingDeletes(Mapping $mapping, array $source_keys, array $destination_keys)
	{
		$filtered_source_keys = $this->filterKeys($mapping, $source_keys);
		$destination_keys     = array_udiff($destination_keys, $filtered_source_keys, function($a, $b) {
			return $a != $b ? ($a > $b ? 1 : -1) : 0;
		});

		if (!count($destination_keys)) {
			return NULL;
		}

		foreach (array_chunk($destination_keys, 1000) as $destination_keys) {
			$destination_delete_query = $mapping->composeDestinationDeleteQuery($destination_keys);

			try {
				$delete_results = $this->destination->query($destination_delete_query);
			} catch (\Exception $e) {
				echo sprintf(
					"Failed removing destination records with query: %s  The database returned: %s",
					$destination_delete_query,
					$e->getMessage()
				);
			}
		}
	}


	/**
	 *
	 */
	protected function syncMappingInserts(Mapping $mapping, array $source_keys, array $destination_keys)
	{
		$insert_results = array();

		foreach ($this->filterKeys($mapping, $source_keys) as $i => $key) {
			if (in_array($key, $destination_keys)) {
				unset($source_keys[$i]);
			}
		}

		if (!count($source_keys)) {
			return NULL;
		}

		foreach (array_chunk($source_keys, 1000) as $source_keys) {
			$source_select_query = $mapping->composeSourceSelectQuery($source_keys);

			try {
				$insert_results = $this->source->query($source_select_query, PDO::FETCH_ASSOC);
			} catch (\Exception $e) {
				echo sprintf(
					"Failed selecting insert results with query: %s  The database returned: %s",
					$source_select_query,
					$e->getMessage()
				);
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
					echo sprintf(
						'Failed inserting into %s with the following: %s  The database returned: %s',
						ucwords(str_replace('_', ' ', $mapping->getDestination())),
						json_encode($this->filter($mapping, $row)),
						$e->getMessage()
					);
				}
			}
		}
	}


	/**
	 *
	 */
	protected function syncMappingUpdates(Mapping $mapping, array $source_keys, array $destination_keys)
	{
		$update_results = array();

		if (!$destination_keys) {
			$source_keys = $this->getUpdatedSourceKeys($mapping, $source_keys);
		} else {
			foreach ($this->filterKeys($mapping, $source_keys) as $i => $key) {
				if (!in_array($key, $destination_keys)) {
					unset($source_keys[$i]);
				}
			}
		}

		if (!count($source_keys)) {
			return NULL;
		}

		foreach (array_chunk($source_keys, 1000) as $source_keys) {
			$source_select_query = $mapping->composeSourceSelectQuery($source_keys);

			try {
				$update_results = $this->source->query($source_select_query, PDO::FETCH_ASSOC);
			} catch (\Exception $e) {
				echo sprintf(
					"Failed selecting update results with query: %s  The database returned: %s",
					$source_select_query,
					$e->getMessage()
				);
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
					echo sprintf(
						'Failed updating %s with the following: %s  The database returned: %s',
						ucwords(str_replace('_', ' ', $mapping->getDestination())),
						json_encode($this->filter($mapping, $row)),
						$e->getMessage()
					);
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
