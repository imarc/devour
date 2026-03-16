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
	protected $fileDriver = NULL;


	/**
	 *
	 */
	protected $fileMaterializedSources = [];


	/**
	 *
	 */
	public function __construct(PDO $database, $strict_time = FALSE, $chunk_limit = 5000)
	{
		parent::__construct($database, $database, $strict_time, $chunk_limit);

		$this->fileDriver = new CsvDriver();
	}


	/**
	 *
	 */
	public function setFileDriver(FileDriver $file_driver)
	{
		$this->fileDriver = $file_driver;

		return $this;
	}


	/**
	 *
	 */
	public function run(array $mappings = array(), $ids = array(), $force_update = FALSE): array
	{
		$this->fileMaterializedSources = [];

		return parent::run($mappings, $ids, $force_update);
	}


	/**
	 *
	 */
	public function runWithDriver(FileDriver $file_driver, array $mappings = array(), $ids = array(), $force_update = FALSE): array
	{
		$this->setFileDriver($file_driver);

		return $this->run($mappings, $ids, $force_update);
	}


	/**
	 *
	 */
	protected function beforeSyncMapping(Mapping $mapping)
	{
		$this->prepareFileSource($mapping);
	}


	/**
	 *
	 */
	protected function getTransferSelectDatabase(Mapping $mapping)
	{
		if ($this->isFileMapping($mapping)) {
			return $this->destination;
		}

		return parent::getTransferSelectDatabase($mapping);
	}


	/**
	 *
	 */
	protected function getTransferSelectDatabaseName(Mapping $mapping)
	{
		if ($this->isFileMapping($mapping)) {
			return 'destination';
		}

		return parent::getTransferSelectDatabaseName($mapping);
	}


	/**
	 *
	 */
	protected function getFileDriver()
	{
		if ($this->fileDriver === NULL) {
			$this->fileDriver = new CsvDriver();
		}

		return $this->fileDriver;
	}


	/**
	 *
	 */
	protected function prepareFileSource(Mapping $mapping)
	{
		if (!$this->isFileMapping($mapping)) {
			return;
		}

		$destination = $mapping->getDestination();
		if (isset($this->fileMaterializedSources[$destination])) {
			$mapping->setSource($this->fileMaterializedSources[$destination]);
			return;
		}

		$alias = $this->getFileDriver()->getAlias($mapping);

		$table = $this->getFileDriver()->materialize($this->destination, $mapping);
		$source = sprintf('%s %s', $table, $alias);

		$mapping->setSource($source);
		$this->fileMaterializedSources[$destination] = $source;
	}


	/**
	 *
	 */
	protected function isFileMapping(Mapping $mapping)
	{
		return $this->getFileDriver()->supports($mapping);
	}
}
