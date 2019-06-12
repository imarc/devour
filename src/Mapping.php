<?php

namespace Devour;

/**
 *
 */
class Mapping
{
	/**
	 *
	 */
	protected $fields = array();


	/**
	 *
	 */
	protected $filters = array();


	/**
	 *
	 */
	protected $joins = array();


	/**
	 *
	 */
	protected $key = NULL;


	/**
	 *
	 */
	protected $params = array();


	/**
	 *
	 */
	protected $destination = NULL;


	/**
	 *
	 */
	protected $requirements = array();


	/**
	 *
	 */
	protected $source = NULL;


	/**
	 *
	 */
	protected $updateWheres = array();


	/**
	 *
	 */
	protected $wheres = array();


	/**
	 *
	 */
	public function __construct($source, $destination, $key)
	{
		$this->key         = $key;
		$this->source      = $source;
		$this->destination = $destination;
	}

	/**
	 *
	 */
	public function addField($alias, $target)
	{
		$this->fields[$alias] = $target;

		return $this;
	}


	/**
	 *
	 */
	public function addFilter($alias, $filter)
	{
		if (!isset($this->filters[$alias])) {
			$this->filters[$alias] = array();
		}

		$this->filters[$alias][] = $filter;

		return $this;
	}


	/**
	 *
	 */
	public function addJoin($alias, $target, $conditions)
	{
		if (!isset($this->joins[$target])) {
			$this->joins[$target] = array();
		}

		$this->joins[$target][$alias] = $conditions;

		return $this;
	}


	/**
	 *
	 */
	public function addParam($name, $value)
	{
		$this->params[$name] = $value;

		return $this;
	}


	/**
	 *
	 */
	public function addRequirement($requirement)
	{
		$this->requirements[] = $requirement;

		return $this;
	}


	/**
	 *
	 */
	public function addWhere($condition, $update_only = FALSE)
	{
		if ($update_only) {
			$this->updateWheres[] = $condition;
		} else {
			$this->wheres[] = $condition;
		}

		return $this;
	}


	/**
	 *
	 */
	public function compose($string, ...$params)
	{
		$string = sprintf($string, ...$params);

		foreach ($this->params as $param => $value) {
			$string = preg_replace('#\{\{\s*' . $param . '\s*\}\}#', $value, $string);
		}

		return $string;
	}


	/**
	 *
	 */
	public function composeDestinationDeleteQuery(array $keys)
	{
		return $this->compose(
			'DELETE FROM %s WHERE %s',
			$this->destination,
			$this->makeDestinationInKeys($keys)
		);
	}


	/**
	 *
	 */
	public function composeDestinationExistingKeysQuery()
	{
		return $this->compose(
			'SELECT %s FROM %s',
			$this->key,
			$this->destination
		);
	}


	/**
	 *
	 */
	public function composeSourceExistingKeysQuery()
	{
		return $this->compose(
			'SELECT %s FROM %s WHERE %s',
			$this->makeSourceKey(),
			$this->makeSourceFrom(),
			$this->makeSourceWheres()
		);
	}


	/**
	 *
	 */
	public function composeSourceSelectQuery($keys)
	{
		return $this->compose(
			'SELECT %s FROM %s WHERE %s AND %s',
			$this->makeSourceFields(),
			$this->makeSourceFrom(),
			$this->makeSourceWheres(),
			$this->makeSourceInKeys($keys)
		);
	}


	/**
	 *
	 */
	public function composeSourceUpdatedKeysQuery(array $existing_keys)
	{
		return $this->compose(
			'SELECT %s FROM %s WHERE %s AND %s',
			$this->makeSourceKey(),
			$this->makeSourceFrom(),
			$this->makeSourceUpdateWheres(),
			$this->makeSourceInKeys($existing_keys)
		);
	}


	/**
	 *
	 */
	public function getDestination()
	{
		return $this->destination;
	}


	/**
	 *
	 */
	public function getKey()
	{
		return $this->key;
	}


	/**
	 *
	 */
	public function getFilters($alias)
	{
		return array_merge(
			$this->filters['*'] ?? [],
			$this->filters[$alias] ?? []
		);
	}


	/**
	 *
	 */
	public function getRequirements()
	{
		return $this->requirements;
	}


	/**
	 *
	 */
	protected function makeDestinationInKeys(array $keys)
	{
		return sprintf(
			'%s IN(%s)',
			$this->key,
			implode(', ', array_map(function($key) {
				if (is_string($key)) {
					return sprintf("'%s'", $key);
				} else {
					return $key;
				}
			}, $keys))
		);
	}


	/**
	 *
	 */
	protected function makeSourceFields()
	{
		$fields = array();

		foreach ($this->fields as $alias => $target) {
			$fields[] = sprintf('%s as %s', $target, $alias);
		}

		return implode(', ', $fields);
	}


	/**
	 *
	 */
	protected function makeSourceFrom()
	{
		$source = [sprintf('%s %s', $this->source, $this->destination)];

		return implode(' LEFT JOIN ',  array_merge($source, $this->makeSourceJoins()));
	}


	/**
	 *
	 */
	protected function makeSourceInKeys(array $keys)
	{
		return sprintf(
			'%s IN(%s)',
			$this->fields[$this->key],
			implode(', ', array_map(function($key) {
				if (is_string($key)) {
					return sprintf("'%s'", $key);
				} else {
					return $key;
				}
			}, $keys))
		);
	}


	/**
	 *
	 */
	protected function makeSourceJoins()
	{
		$joins = array();

		foreach ($this->joins as $target => $mapping) {
			foreach ($mapping as $alias => $conditions) {
				$joins[] = sprintf('%s %s ON %s', $target, $alias, implode(' AND ', $conditions));
			}
		}

		return $joins;
	}


	/**
	 *
	 */
	protected function makeSourceKey()
	{
		return sprintf('%s as %s', $this->fields[$this->key], $this->key);
	}


	/**
	 *
	 */
	protected function makeSourceUpdateWheres()
	{
		return implode(' AND ', $this->updateWheres) ?: 'NULL IS NULL';
	}


	/**
	 *
	 */
	protected function makeSourceWheres()
	{
		return implode(' AND ', $this->wheres) ?: 'NULL IS NULL';
	}
}
