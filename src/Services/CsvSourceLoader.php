<?php

namespace Devour;

use PDO;
use RuntimeException;

/**
 *
 */
class CsvSourceLoader
{
	/**
	 *
	 */
	public function materialize(PDO $database, Mapping $mapping)
	{
		$config = $mapping->getCsvConfig() ?: [];

		if (empty($config['path'])) {
			throw new RuntimeException(sprintf(
				'Cannot materialize CSV source for "%s", path is required.',
				$mapping->getDestination()
			));
		}

		$path = $config['path'];

		if (!is_readable($path)) {
			throw new RuntimeException(sprintf(
				'Cannot read CSV source "%s" for "%s".',
				$path,
				$mapping->getDestination()
			));
		}

		$handle = $this->openCsv($path);

		$header     = [];
		$has_header = !empty($config['header']);

		if ($has_header) {
			$header = $this->readHeader($handle, $config);
		}

		$columns = $this->normalizeColumns($config['columns'] ?? []);
		if (empty($columns) && $has_header) {
			$columns = $this->buildColumnsFromHeader($header);
		}

		if (empty($columns)) {
			$columns = $this->inferColumnsFromMapping($mapping);
		}

		if (empty($columns)) {
			throw new RuntimeException(sprintf(
				'Cannot infer CSV columns for "%s". Configure csv.columns or enable csv.header.',
				$mapping->getDestination()
			));
		}

		$driver = $this->getDriverName($database);
		$table  = $this->buildTableName($mapping, $config, $driver);

		$this->createTable($database, $table, $columns, $driver);
		$this->insertRows($database, $table, array_keys($columns), $handle, $config);

		fclose($handle);

		return $table;
	}


	/**
	 *
	 */
	protected function buildColumnsFromHeader(array $header)
	{
		$columns = [];

		foreach ($header as $index => $name) {
			$column = $this->sanitizeIdentifier($name ?: ('column_' . ($index + 1)));
			$columns[$column] = 'text';
		}

		return $columns;
	}


	/**
	 *
	 */
	protected function buildTableName(Mapping $mapping, array $config, $driver)
	{
		if (!empty($config['table'])) {
			$table = trim($config['table']);

			if ($this->isSqlServerDriver($driver) && substr($table, 0, 1) === '#') {
				return '#' . $this->sanitizeIdentifier(substr($table, 1));
			}

			return $this->sanitizeIdentifier($table);
		}

		$prefix = $this->isSqlServerDriver($driver) ? '#' : '';

		return sprintf(
			'%sdevour_csv_%s_%s',
			$prefix,
			$this->sanitizeIdentifier($mapping->getDestination()),
			substr(md5(uniqid('', TRUE)), 0, 8)
		);
	}


	/**
	 *
	 */
	protected function createTable(PDO $database, $table, array $columns, $driver)
	{
		if ($this->isSqlServerDriver($driver) && substr($table, 0, 1) === '#') {
			$database->query(sprintf("IF OBJECT_ID('tempdb..%s') IS NOT NULL DROP TABLE %s", $table, $table));
		} else {
			$database->query(sprintf('DROP TABLE IF EXISTS %s', $table));
		}

		$column_chunks = [];
		foreach ($columns as $column => $type) {
			$column_chunks[] = sprintf('%s %s', $column, $this->normalizeColumnType($type, $driver));
		}

		$create_query = 'CREATE TEMPORARY TABLE %s (%s)';
		if ($this->isSqlServerDriver($driver)) {
			$create_query = 'CREATE TABLE %s (%s)';
		}

		$database->query(sprintf($create_query, $table, join(', ', $column_chunks)));
	}


	/**
	 *
	 */
	protected function inferColumnsFromMapping(Mapping $mapping)
	{
		$columns = [];

		foreach ($mapping->getFields() as $alias => $target) {
			$columns[$this->sanitizeIdentifier($alias)] = 'text';
		}

		return $columns;
	}


	/**
	 *
	 */
	protected function insertRows(PDO $source, $table, array $columns, $handle, array $config)
	{
		$insert_statement = $source->prepare(sprintf(
			'INSERT INTO %s (%s) VALUES (%s)',
			$table,
			join(', ', $columns),
			':' . join(', :', $columns)
		));

		while (($row = fgetcsv(
			$handle,
			0,
			$config['delimiter'] ?? ',',
			$config['enclosure'] ?? '"',
			$config['escape'] ?? '\\'
		)) !== FALSE) {
			$data = array_pad($row, count($columns), NULL);

			foreach ($columns as $index => $column) {
				$value = $data[$index] ?? NULL;
				$insert_statement->bindValue(':' . $column, $value !== '' ? $value : NULL);
			}

			$insert_statement->execute();
		}
	}


	/**
	 *
	 */
	protected function normalizeColumns(array $columns)
	{
		$normalized = [];

		foreach ($columns as $name => $type) {
			if (is_int($name)) {
				$normalized[$this->sanitizeIdentifier($type)] = 'text';
			} else {
				$normalized[$this->sanitizeIdentifier($name)] = $type;
			}
		}

		return $normalized;
	}


	/**
	 *
	 */
	protected function normalizeColumnType($type, $driver)
	{
		$type = trim(strtolower((string) $type));

		if ($this->isSqlServerDriver($driver) && $type === 'text') {
			return 'nvarchar(max)';
		}

		return $type;
	}


	/**
	 *
	 */
	protected function getDriverName(PDO $database)
	{
		$driver = (string) $database->getAttribute(PDO::ATTR_DRIVER_NAME);

		if ($driver === 'odbc') {
			try {
				$statement = $database->query('SELECT @@VERSION as version');
				$version   = $statement ? (string) $statement->fetchColumn() : '';
				if (stripos($version, 'microsoft sql server') !== FALSE) {
					return 'sqlsrv';
				}
			} catch (\Exception $e) {
				return $driver;
			}
		}

		return $driver;
	}


	/**
	 *
	 */
	protected function isSqlServerDriver($driver)
	{
		return in_array($driver, ['sqlsrv', 'dblib']);
	}


	/**
	 *
	 */
	protected function openCsv($path)
	{
		$handle = fopen($path, 'r');
		if ($handle === FALSE) {
			throw new RuntimeException(sprintf('Cannot open CSV source "%s".', $path));
		}

		return $handle;
	}


	/**
	 *
	 */
	protected function readHeader($handle, array $config)
	{
		$header = fgetcsv(
			$handle,
			0,
			$config['delimiter'] ?? ',',
			$config['enclosure'] ?? '"',
			$config['escape'] ?? '\\'
		);

		if ($header === FALSE) {
			throw new RuntimeException('Cannot read CSV header row.');
		}

		return $header;
	}


	/**
	 *
	 */
	protected function sanitizeIdentifier($name)
	{
		$name = strtolower(trim((string) $name));
		$name = preg_replace('/[^a-z0-9_]+/', '_', $name);
		$name = trim($name, '_');

		if ($name === '' || ctype_digit($name[0])) {
			$name = 'col_' . $name;
		}

		return $name;
	}
}
