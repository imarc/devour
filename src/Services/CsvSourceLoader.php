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
	public function materialize(PDO $source, Mapping $mapping)
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

		$table = $this->buildTableName($mapping, $config);

		$this->createTable($source, $table, $columns);
		$this->insertRows($source, $table, array_keys($columns), $handle, $config);

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
	protected function buildTableName(Mapping $mapping, array $config)
	{
		if (!empty($config['table'])) {
			return $this->sanitizeIdentifier($config['table']);
		}

		return sprintf(
			'devour_csv_%s_%s',
			$this->sanitizeIdentifier($mapping->getDestination()),
			substr(md5(uniqid('', TRUE)), 0, 8)
		);
	}


	/**
	 *
	 */
	protected function createTable(PDO $source, $table, array $columns)
	{
		$source->query(sprintf('DROP TABLE IF EXISTS %s', $table));

		$column_chunks = [];
		foreach ($columns as $column => $type) {
			$column_chunks[] = sprintf('%s %s', $column, $type);
		}

		$source->query(sprintf(
			'CREATE TEMPORARY TABLE %s (%s)',
			$table,
			join(', ', $column_chunks)
		));
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
