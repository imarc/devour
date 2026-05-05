<?php

use PHPUnit\Framework\TestCase;

class TestFileDriver implements Devour\FileDriver
{
	public $calls = 0;
	public $supports = TRUE;

	public function supports(Devour\Mapping $mapping)
	{
		return $this->supports;
	}

	public function getAlias(Devour\Mapping $mapping)
	{
		return 'filesrc';
	}

	public function materialize(PDO $database, Devour\Mapping $mapping)
	{
		$this->calls++;

		return 'devour_file_events_test';
	}
}

class TestImporter extends Devour\Importer
{
	protected function hasStatsTable()
	{
		return TRUE;
	}

	protected function hasUpdatesTable()
	{
		return TRUE;
	}

	public function callBeforeSyncMapping(Devour\Mapping $mapping)
	{
		$this->beforeSyncMapping($mapping);
	}

	public function callTransferSelectDatabaseName(Devour\Mapping $mapping)
	{
		return $this->getTransferSelectDatabaseName($mapping);
	}

	public function callFileDriverClass()
	{
		return get_class($this->getFileDriver());
	}
}

final class ImporterTest extends TestCase
{
	public function testCsvMappingStagesOncePerDestination()
	{
		$database = new PDO('sqlite::memory:');
		$driver   = new TestFileDriver();
		$importer = new TestImporter($database);
		$importer->setFileDriver($driver);

		$mapping = new Devour\Mapping('placeholder', 'events', 'id');
		$mapping->setFileConfig('stub', [
			'path' => '/tmp/events.csv'
		]);

		$importer->callBeforeSyncMapping($mapping);
		$importer->callBeforeSyncMapping($mapping);

		$this->assertEquals(1, $driver->calls);
		$this->assertEquals('devour_file_events_test filesrc', $mapping->getSource());
	}

	public function testFileTransferSelectUsesDestinationName()
	{
		$database = new PDO('sqlite::memory:');
		$driver   = new TestFileDriver();
		$importer = new TestImporter($database);
		$importer->setFileDriver($driver);

		$file_mapping = new Devour\Mapping('placeholder', 'events', 'id');
		$file_mapping->setFileConfig('stub', [
			'path' => '/tmp/events.csv'
		]);

		$db_mapping = new Devour\Mapping('source_table', 'events', 'id');
		$driver->supports = FALSE;

		$this->assertEquals('source', $importer->callTransferSelectDatabaseName($db_mapping));

		$driver->supports = TRUE;

		$this->assertEquals('destination', $importer->callTransferSelectDatabaseName($file_mapping));
	}

	public function testRunCanSetDriverAtRuntime()
	{
		$database = new PDO('sqlite::memory:');
		$database->exec('CREATE TABLE devour_stats (id INTEGER PRIMARY KEY AUTOINCREMENT, start_time TEXT, scheduled_by TEXT, scheduled_time TEXT, end_time TEXT, tables TEXT, ids TEXT, force INTEGER, log TEXT)');
		$database->exec('CREATE TABLE devour_updates (target VARCHAR(255) PRIMARY KEY, time TEXT)');

		$importer = new TestImporter($database);
		$driver   = new TestFileDriver();

		$importer->runWithDriver($driver);

		$this->assertEquals(TestFileDriver::class, $importer->callFileDriverClass());
	}

}
