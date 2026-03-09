<?php

namespace Devour;

use PDO;

/**
 *
 */
class Importer extends Synchronizer
{
	/**
	 *
	 */
	protected $csvLoader = NULL;


	/**
	 *
	 */
	protected $csvMaterializedSources = [];


	/**
	 *
	 */
	public function __construct(PDO $database, $strict_time = FALSE, $chunk_limit = 5000)
	{
		parent::__construct($database, $database, $strict_time, $chunk_limit);
	}


	/**
	 *
	 */
	public function setCsvSourceLoader(CsvSourceLoader $csv_loader)
	{
		$this->csvLoader = $csv_loader;

		return $this;
	}


	/**
	 *
	 */
	protected function beforeSyncMapping(Mapping $mapping)
	{
		$this->prepareCsvSource($mapping);
	}


	/**
	 *
	 */
	protected function getTransferSelectDatabase(Mapping $mapping)
	{
		if ($mapping->isCsvSource()) {
			return $this->destination;
		}

		return parent::getTransferSelectDatabase($mapping);
	}


	/**
	 *
	 */
	protected function getTransferSelectDatabaseName(Mapping $mapping)
	{
		if ($mapping->isCsvSource()) {
			return 'destination';
		}

		return parent::getTransferSelectDatabaseName($mapping);
	}


	/**
	 *
	 */
	protected function getCsvSourceLoader()
	{
		if ($this->csvLoader === NULL) {
			$this->csvLoader = new CsvSourceLoader();
		}

		return $this->csvLoader;
	}


	/**
	 *
	 */
	protected function prepareCsvSource(Mapping $mapping)
	{
		if (!$mapping->isCsvSource()) {
			return;
		}

		$destination = $mapping->getDestination();
		if (isset($this->csvMaterializedSources[$destination])) {
			$mapping->setSource($this->csvMaterializedSources[$destination]);
			return;
		}

		$config = $mapping->getCsvConfig();
		$alias  = $config['alias'] ?? 'csvsrc';

		$table = $this->getCsvSourceLoader()->materialize($this->destination, $mapping);
		$source = sprintf('%s %s', $table, $alias);

		$mapping->setSource($source);
		$this->csvMaterializedSources[$destination] = $source;
	}
}
