<?php

use PHPUnit\Framework\TestCase;

final class SynchronizerTest extends TestCase
{
	public function testScheduleStoresForceAndTables(): void
	{
		$database = new PDO('sqlite::memory:');
		$database->exec('CREATE TABLE devour_stats (id INTEGER PRIMARY KEY AUTOINCREMENT, start_time TEXT, scheduled_by TEXT, scheduled_time TEXT, end_time TEXT, tables TEXT, ids TEXT, force INTEGER, log TEXT)');
		$database->exec('CREATE TABLE devour_updates (target VARCHAR(255) PRIMARY KEY, time TIMESTAMP)');

		$sync = new Devour\Synchronizer($database, $database);
		$sync->schedule(['events', 'people'], [], 'admin@example.com', TRUE);

		$result = $database->query('SELECT tables, force FROM devour_stats LIMIT 1')->fetch(PDO::FETCH_ASSOC);

		$this->assertSame('["events","people"]', $result['tables']);
		$this->assertTrue((bool) $result['force']);
	}

	public function testRunUsesScheduledTablesAndForce(): void
	{
		$database = new PDO('sqlite::memory:');
		$database->exec('CREATE TABLE devour_stats (id INTEGER PRIMARY KEY AUTOINCREMENT, start_time TEXT, scheduled_by TEXT, scheduled_time TEXT, end_time TEXT, tables TEXT, ids TEXT, force INTEGER, log TEXT)');
		$database->exec('CREATE TABLE devour_updates (target VARCHAR(255) PRIMARY KEY, time TIMESTAMP)');

		$sync = new class($database, $database) extends Devour\Synchronizer {
			public $calls = [];

			protected function syncMapping($name, $ids, $force_update, $context = NULL)
			{
				$this->calls[] = compact('name', 'force_update');
			}
		};

		$sync->addMapping(new Devour\Mapping('source_events', 'events', 'id'));
		$sync->addMapping(new Devour\Mapping('source_people', 'people', 'id'));
		$sync->schedule(['events'], [], 'admin@example.com', TRUE);
		$sync->run();

		$this->assertCount(1, $sync->calls);
		$this->assertSame('events', $sync->calls[0]['name']);
		$this->assertTrue($sync->calls[0]['force_update']);
	}


	public function testStatUpdateNormalizesFalseForce(): void
	{
		$database = new PDO('sqlite::memory:');
		$database->exec('CREATE TABLE devour_stats (id INTEGER PRIMARY KEY AUTOINCREMENT, start_time TEXT, scheduled_by TEXT, scheduled_time TEXT, end_time TEXT, tables TEXT, ids TEXT, force INTEGER, log TEXT)');
		$database->exec('CREATE TABLE devour_updates (target VARCHAR(255) PRIMARY KEY, time TIMESTAMP)');
		$database->exec("INSERT INTO devour_stats VALUES (1, NULL, NULL, '2026-08-14 00:00:00', NULL, NULL, NULL, 0, NULL)");

		$sync = new class($database, $database) extends Devour\Synchronizer {
			public function setStat(array $stat): void
			{
				$this->stat = $stat;
			}

			public function updateStat(string $column, string $value): void
			{
				$this->statSet($column, $value);
			}
		};
		$sync->setStat([
			'id' => 1,
			'start_time' => NULL,
			'scheduled_by' => NULL,
			'scheduled_time' => '2026-08-14 00:00:00',
			'end_time' => NULL,
			'tables' => NULL,
			'ids' => NULL,
			'force' => FALSE,
			'log' => NULL
		]);
		$sync->updateStat('log', 'Updated');

		$this->assertSame(0, $database->query('SELECT force FROM devour_stats WHERE id = 1')->fetchColumn());
	}
}
