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
	protected $dependencies = array();


	/**
	 *
	 */
	protected $destination = NULL;


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

		settype($this->key, 'array');
	}


	/**
	 *
	 */
	public function addDependency($dependency)
	{
		$this->dependencies[] = $dependency;

		return $this;
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
	public function addUpdateWhere($condition)
	{
		$this->updateWheres[] = $condition;

		return $this;
	}


	/**
	 *
	 */
	public function addWhere($condition)
	{
		$this->wheres[] = $condition;

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
		$sql = $this->compose(
			'SELECT %s FROM %s',
			$this->makeDestinationKey(),
			$this->destination
		);

		return $sql;
	}


	/**
	 *
	 */
	public function composeSourceExistingKeysQuery()
	{
		$sql = $this->compose(
			'SELECT %s FROM %s WHERE %s',
			$this->makeSourceKey(),
			$this->makeSourceFrom(),
			$this->makeSourceWheres()
		);

		return $sql;
	}


	/**
	 *
	 */
	public function composeSourceSelectQuery($keys)
	{
		$sql = $this->compose(
			'SELECT %s FROM %s WHERE %s AND %s',
			$this->makeSourceFields(),
			$this->makeSourceFrom(),
			$this->makeSourceWheres(),
			$this->makeSourceInKeys($keys)
		);

		return $sql;
	}


	/**
	 *
	 */
	public function composeSourceUpdatedKeysQuery(array $existing_keys)
	{
		$sql = $this->compose(
			'SELECT %s FROM %s WHERE (%s) AND %s',
			$this->makeSourceKey(),
			$this->makeSourceFrom(),
			$this->makeSourceUpdateWheres(),
			$this->makeSourceInKeys($existing_keys)
		);

		return $sql;
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
	public function getDependencies()
	{
		return $this->dependencies;
	}


	/**
	 *
	 */
	protected function makeDestinationInKeys(array $keys)
	{
		return sprintf('(%s)', implode(' OR ', array_map(function($key) {
			$group = count($this->key) > 1 ? '(%s)' : '%s';

			return sprintf($group, implode(' AND ', array_map(function($field) use ($key) {
				if (is_string($key[$field])) {
					return sprintf("%s = '%s'", $field, str_replace("'", "''", $key[$field]));
				} else {
					return sprintf("%s = %s", $field, $key[$field]);
				}
			}, $this->key)));
		}, $keys)));
	}


	/**
	 *
	 */
	protected function makeDestinationKey()
	{
		return join(', ', $this->key);
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
		if (preg_match('/.+\s+.+/', $this->source)) {
			$source = [$this->source];
		} else {
			$source = [sprintf('%s %s', $this->source, $this->source)];
		}

		return implode(' LEFT JOIN ',  array_merge($source, $this->makeSourceJoins()));
	}


	/**
	 *
	 */
	protected function makeSourceInKeys(array $keys)
	{
		return sprintf('(%s)', implode(' OR ', array_map(function($key) {
			$group = count($this->key) > 1 ? '(%s)' : '%s';

			return sprintf($group, implode(' AND ', array_map(function($field) use ($key) {
				if (is_string($key[$field])) {
					return sprintf("%s = '%s'", $this->fields[$field], str_replace("'", "''", $key[$field]));
				} else {
					return sprintf("%s = %s", $this->fields[$field], $key[$field]);
				}
			}, $this->key)));
		}, $keys)));
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
		return join(', ', array_map(function($field) {
			return sprintf('%s as %s', $this->fields[$field], $field);
		}, $this->key));
	}


	/**
	 *
	 */
	protected function makeSourceUpdateWheres()
	{
		return implode(' OR ', $this->updateWheres) ?: 'NULL IS NULL';
	}


	/**
	 *
	 */
	protected function makeSourceWheres()
	{
		return implode(' AND ', $this->wheres) ?: 'NULL IS NULL';
	}
}
