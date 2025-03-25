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
	protected $destinationOrderBys = array();


	/**
	 *
	 */
	protected $contextFields = array();


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
	protected $generators = array();


	/**
	 *
	 */
	protected $immutable = FALSE;


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
	protected $persistent = FALSE;


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
	public function addContext($alias, $target)
	{
		$this->contextFields[$alias] = $target;

		return $this;
	}


	/**
	 * 
	 */
	public function addDestinationOrderBy($field, $direction) {
		$this->destinationOrderBys[$field] = $direction;
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
	public function canInsert()
	{
		return TRUE;
	}


	/**
	 *
	 */
	public function canUpdate()
	{
		return !$this->isImmutable();
	}


	/**
	 *
	 */
	public function canDelete()
	{
		return !$this->isImmutable() && !$this->isPersistent();
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
	public function composeDestinationExistingKeysQuery()
	{
		$sql = $this->compose(
			'SELECT %s FROM %s',
			$this->makeDestinationKey(),
			$this->getDestination()
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
	public function composeSourceInsertSelectQuery()
	{
		$keys = [];
		foreach ($this->key as $key) {
			$keys[] = sprintf('devour_temp_%s.%s = %s.%s', $this->getDestination(), $key, $this->getDestination(), $key);
		}

		$join_keys = join(' AND ', $keys);

		$sql = $this->compose(
			'SELECT %s from devour_temp_%s LEFT OUTER JOIN %s ON (%s) WHERE %s.%s IS NULL',
			$this->makeTemporaryFields(),
			$this->getDestination(),
			$this->getDestination(),
			$join_keys,
			$this->getDestination(),
			$this->key[0]
		);

		return $sql;
	}

	/**
	 * 
	 */
	public function composeSourceDeleteSelectQuery()
	{
		$keys = [];
		foreach ($this->getKey() as $key) {
			$keys[] = sprintf('devour_temp_%s.%s = %s.%s', $this->getDestination(), $key, $this->getDestination(), $key);
		}

		$join_keys = join(' AND ', $keys);

		$sql = $this->compose(
			'SELECT %s.* from devour_temp_%s RIGHT OUTER JOIN %s ON (%s) WHERE devour_temp_%s.%s IS NULL',
			$this->getDestination(),
			$this->getDestination(),
			$this->getDestination(),
			$join_keys,
			$this->getDestination(),
			$this->key[0]
		);

		return $sql;
	}


	/**
	 * 
	 */
	public function composeSourceUpdateSelectQuery($force = FALSE, $limit = NULL, $offset = NULL)
	{
		$keys = [];
		foreach ($this->key as $key) {
			$keys[] = sprintf('devour_temp_%s.%s = %s.%s', $this->getDestination(), $key, $this->getDestination(), $key);
		}

		$join_keys = join(' AND ', $keys);

		if (count($this->destinationOrderBys)) {
			$chunk = ' ORDER BY ';
			$chunk .= join(', ', array_map(function ($field, $direction) {
				return sprintf('%s %s', $field, $direction);
			}, array_keys($this->destinationOrderBys), array_values($this->destinationOrderBys)));

			$chunk .= $limit  !== NULL ? sprintf(' LIMIT %d', $limit)   : '';
			$chunk .= $offset !== NULL ? sprintf(' OFFSET %d', $offset) : '';
		}
		
		$sql = $this->compose(
			'SELECT %s from devour_temp_%s inner JOIN %s ON (%s) WHERE %s.%s IS NOT NULL %s %s',
			$this->makeUpdateFields(),
			$this->getDestination(),
			$this->getDestination(),
			$join_keys,
			$this->getDestination(),
			$this->key[0],
			$force ? '' : ' AND devour_updated = TRUE',
			$chunk ?? ''
		);

		return $sql;
	}


	/**
	 *
	 */
	public function composeSourceSelectQuery($keys = NULL)
	{
		$sql    = 'SELECT %s FROM %s WHERE %s';
		$params = [
			$this->makeSourceFields(),
			$this->makeSourceFrom(),
			$this->makeSourceWheres()
		];

		if ($keys) {
			$sql .= ' AND %s';
			$params[] = $this->makeSourceInKeys($keys);
		}
		
		$sql = $this->compose(
			$sql,
			...$params
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
	public function getContextFields()
	{
		return $this->contextFields;
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
	public function getGenerators()
	{
		return $this->generators;
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
	public function isChunked()
	{
		return count($this->destinationOrderBys);
	}


	/**
	 *
	 */
	public function isImmutable()
	{
		return $this->immutable;
	}


	/**
	 *
	 */
	public function isPersistent()
	{
		return $this->persistent;
	}


	/**
	 *
	 */
	public function setGenerator($alias, $generator)
	{
		$this->generators[$alias] = $generator;

		return $this;
	}


	/**
	 *
	 */
	public function setImmutable($value)
	{
		$this->immutable = $value;
		return $this;
	}


	/**
	 *
	 */
	public function setPersistent($value)
	{
		$this->persistent = $value;
		return $this;
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
	protected function makeDestinationOrderBys()
	{
		
	}


	/**
	 * 
	 */
	protected function makeTemporaryFields($contextual = TRUE)
	{
		$fields = $contextual ? array_merge($this->fields, $this->contextFields) : $this->fields;

		return join(', ', array_map(function($field) {
			return sprintf("devour_temp_%s.%s", $this->getDestination(), $field);
		}, array_keys($fields)));
	}


	/**
	 * 
	 */
	protected function makeUpdateFields()
	{
		$fields = array();

		foreach ($this->fields as $alias => $target) {
			$fields[] = sprintf('devour_temp_%s.%s as %s', $this->getDestination(), $alias, $alias);
		}

		foreach ($this->contextFields as $alias => $target) {
			$fields[] = sprintf('devour_temp_%s.%s as %s', $this->getDestination(), $alias, $alias);
		}

		return implode(', ', $fields);
	}


	/**
	 *
	 */
	protected function makeSourceFields($temp = FALSE)
	{
		$fields = array();

		foreach ($this->fields as $alias => $target) {
			$fields[] = sprintf('%s as %s', $target, $alias);
		}

		foreach ($this->contextFields as $alias => $target) {
			$fields[] = sprintf('%s as %s', $target, $alias);
		}

		if ($temp && count($this->updateWheres)) {
			$fields[] = sprintf('(CASE WHEN %s THEN 1 ELSE 0 END) as devour_updated', $this->makeSourceUpdateWheres());
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
