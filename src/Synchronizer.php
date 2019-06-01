<?php

namespace Devour;

use PDO;
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

		$this->mappings[$destination]   = $mapping;
		$this->synced[$destination]   = FALSE;
		$this->truncate[$destination] = $truncate;
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
	public function run(array $mappings, $force_update = FALSE)
	{
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
		foreach ($row as $column => $value) {
			foreach ($mapping->getFilters($column) as $filter) {
				if (!isset($this->filters[$filter])) {
					throw new RuntimeException(sprintf(
						'Cannot filter column "%s" with "%s", filter not registered.',
						$column,
						$filter
					));
				}

				$row[$column] = $this->filters[$filter]($value);
			}
		}

		return $row;
	}


	/**
	 *
	 */
	protected function getExistingDestinationKeys(Mapping $mapping)
	{
		$keys   = array();
		$result = $this->source->query($mapping->composeDestinationExistingKeysQuery());

		foreach ($result as $row) {
			$keys[] = $row[$mapping->getKey()];
		}

		return $keys;
	}


	/**
	 *
	 */
	protected function getExistingSourceKeys(Mapping $mapping)
	{
		$keys   = array();
		$result = $this->source->query($mapping->composeSourceExistingKeysQuery());

		foreach ($result as $row) {
			$keys[] = $row[$mapping->getKey()];
		}

		return $keys;
	}


	/**
	 *
	 */
	protected function getUpdateSourceKeys(Mapping $mapping, array $source_keys)
	{
		$keys   = array();
		$result = $this->source->query($mapping->composeSourceUpdatedKeysQuery($source_keys));

		foreach ($result as $row) {
			$keys[] = $row[$mapping->getKey()];
		}

		return $keys;
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

		foreach ($mapping->getRequirements() as $requirement) {
			$this->syncMapping($requirement, $force_update);
		}

		$source_keys      = $this->getExistingSourceKeys($mapping);
		$destination_keys = $this->getExistingDestinationKeys($mapping);

		$this->syncMappingDeletes($mapping, $source_keys, $destination_keys);
		$this->syncMappingInserts($mapping, $source_keys, $destination_keys);

		if (!$force_update) {
			$this->syncMappingUpdates($mapping, $this->getUpdateSourceKeys($mapping, $source_keys));
		} else {
			$this->syncMappingUpdates($mapping, array_intersect($source_keys, $destination_keys));
		}

		$this->synced[array_pop($this->stack)] = TRUE;
	}


	/**
	 *
	 */
	protected function syncMappingDeletes(Mapping $mapping, array $source_keys, array $destination_keys)
	{
		if ($this->truncate[$mapping->getDestination()]) {
			return $this->destination->query(sprintf('TRUNCATE TABLE %s', $mapping->getDestination()));
		}

		$destination_delete_query = $mapping->composeDestinationDeleteQuery(array_diff(
			$destination_keys,
			$source_keys
		));

		return $this->destination->query($destination_delete_query);
	}


	/**
	 *
	 */
	protected function syncMappingInserts(Mapping $mapping, array $source_keys, array $destination_keys)
	{
		$source_select_query = $mapping->composeSourceSelectQuery(array_diff(
			$source_keys,
			$destination_keys
		));

		foreach ($this->source->query($source_select_query) as $i => $row) {
			if (!$i) {
				$insert_statement = $this->destination->prepare(sprintf(
					'INSERT INTO %s (%s) VALUES(%s)',
					$mapping->getDestination(),
					$this->composeColumns($row),
					$this->composeParams($row)
				));
			}

			$insert_statement->execute($this->filter($mapping, $row));
		}
	}


	/**
	 *
	 */
	protected function syncMappingUpdates(Mapping $mapping, array $source_keys)
	{
		$source_select_query = $mapping->composeSourceSelectQuery($source_keys);

		foreach ($this->source->query($source_select_query) as $i => $row) {
			if (!$i) {
				$update_statement = $this->destination->prepare(sprintf(
					'UPDATE %s SET %s WHERE %s',
					$mapping->getDestination(),
					$this->composeSetParams($row),
					sprintf('%s = :%s', $mapping->getKey(), $mapping->getKey())
				));
			}

			$update_statement->execute($this->filter($mapping, $row));
		}
	}
}
