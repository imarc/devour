<?php

use PHPUnit\Framework\TestCase;

class TestCsvSourceLoader extends Devour\CsvSourceLoader
{
	public $calls = 0;

	public function materialize(PDO $database, Devour\Mapping $mapping)
	{
		$this->calls++;

		return 'devour_csv_events_test';
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
}

final class ImporterTest extends TestCase
{
	public function testCsvMappingStagesOncePerDestination()
	{
		$database = new PDO('sqlite::memory:');
		$importer = new TestImporter($database);
		$loader   = new TestCsvSourceLoader();

		$mapping = new Devour\Mapping('placeholder', 'events', 'id');
		$mapping->setCsvConfig([
			'path'  => '/tmp/events.csv',
			'alias' => 'csvsrc'
		]);

		$importer->setCsvSourceLoader($loader);
		$importer->callBeforeSyncMapping($mapping);
		$importer->callBeforeSyncMapping($mapping);

		$this->assertEquals(1, $loader->calls);
		$this->assertEquals('devour_csv_events_test csvsrc', $mapping->getSource());
	}

	public function testCsvTransferSelectUsesDestinationName()
	{
		$database = new PDO('sqlite::memory:');
		$importer = new TestImporter($database);

		$csv_mapping = new Devour\Mapping('placeholder', 'events', 'id');
		$csv_mapping->setCsvConfig([
			'path' => '/tmp/events.csv'
		]);

		$db_mapping = new Devour\Mapping('source_table', 'events', 'id');

		$this->assertEquals('destination', $importer->callTransferSelectDatabaseName($csv_mapping));
		$this->assertEquals('source', $importer->callTransferSelectDatabaseName($db_mapping));
	}
}
