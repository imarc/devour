<?php

use PHPUnit\Framework\TestCase;

final class MigrationRunnerIntegrationTest extends TestCase
{
	private $database;

	private $schema;


	protected function setUp(): void
	{
		$dsn = getenv('DEVOUR_TEST_PG_DSN');
		if (!$dsn) {
			$this->markTestSkipped('Set DEVOUR_TEST_PG_DSN to run PostgreSQL migration integration tests.');
		}

		if (!preg_match('/dbname=([^;]+)/', $dsn, $matches) || !str_ends_with($matches[1], '_test')) {
			$this->markTestSkipped('DEVOUR_TEST_PG_DSN must target a database whose name ends in _test.');
		}

		$this->database = new PDO($dsn, getenv('DEVOUR_TEST_PG_USER') ?: NULL, getenv('DEVOUR_TEST_PG_PASSWORD') ?: NULL);
		$this->schema = 'devour_test_' . bin2hex(random_bytes(8));
		$this->database->exec(sprintf('CREATE SCHEMA "%s"', $this->schema));
		$this->database->exec(sprintf('SET search_path TO "%s"', $this->schema));
	}


	protected function tearDown(): void
	{
		if ($this->database && $this->schema) {
			$this->database->exec(sprintf('DROP SCHEMA IF EXISTS "%s" CASCADE', $this->schema));
		}
	}


	public function testMigrateCreatesAndRecordsDevourSchema(): void
	{
		Devour\Migrations\MigrationRunner::migrate($this->database);

		$this->assertSame('devour_stats', $this->database->query("SELECT to_regclass('devour_stats')")->fetchColumn());
		$this->assertSame('devour_updates', $this->database->query("SELECT to_regclass('devour_updates')")->fetchColumn());
		$this->assertSame('1', (string) $this->database->query('SELECT id FROM devour_migrations')->fetchColumn());
	}


	public function testMigrateIsIdempotentAndEnablesRuntimeConsumers(): void
	{
		Devour\Migrations\MigrationRunner::migrate($this->database);
		Devour\Migrations\MigrationRunner::migrate($this->database);

		Devour\Migrations\MigrationRunner::assertReady($this->database);
		$this->assertSame(1, (int) $this->database->query('SELECT COUNT(*) FROM devour_migrations')->fetchColumn());
		$this->assertInstanceOf(Devour\Synchronizer::class, new Devour\Synchronizer($this->database, $this->database));
		$this->assertInstanceOf(Devour\Analyzer::class, new Devour\Analyzer($this->database));
	}


	public function testMigrateBaselinesCurrentLegacySchema(): void
	{
		$this->database->exec('CREATE SEQUENCE devour_stats_id_seq START 100 INCREMENT 5');
		$this->database->exec("CREATE TABLE devour_stats (id INT NOT NULL DEFAULT nextval('devour_stats_id_seq') PRIMARY KEY, start_time TIMESTAMP, scheduled_by TEXT, scheduled_time TIMESTAMP, end_time TIMESTAMP, tables TEXT, ids TEXT, force BOOLEAN, log TEXT)");
		$this->database->exec('ALTER SEQUENCE devour_stats_id_seq OWNED BY devour_stats.id');
		$this->database->exec('CREATE TABLE devour_updates (target VARCHAR(255) PRIMARY KEY, time TIMESTAMP)');

		Devour\Migrations\MigrationRunner::migrate($this->database);

		$this->assertSame('1', (string) $this->database->query('SELECT id FROM devour_migrations')->fetchColumn());
	}


	public function testMigrateBaselinesLegacyUnqualifiedSequenceDefault(): void
	{
		$this->database->exec('CREATE SEQUENCE devour_stats_id_seq START 100 INCREMENT 5');
		$this->database->exec("CREATE TABLE devour_stats (id INT NOT NULL DEFAULT nextval('devour_stats_id_seq') PRIMARY KEY, start_time TIMESTAMP, scheduled_by TEXT, scheduled_time TIMESTAMP, end_time TIMESTAMP, tables TEXT, ids TEXT, force BOOLEAN, log TEXT)");
		$this->database->exec('ALTER SEQUENCE devour_stats_id_seq OWNED BY devour_stats.id');
		$this->database->exec('CREATE TABLE devour_updates (target VARCHAR(255) PRIMARY KEY, time TIMESTAMP)');

		Devour\Migrations\MigrationRunner::migrate($this->database);

		$this->assertSame('1', (string) $this->database->query('SELECT id FROM devour_migrations')->fetchColumn());
	}


	public function testMigrateRejectsIncompleteLegacySchemaWithoutRecordingMigration(): void
	{
		$this->database->exec('CREATE TABLE devour_stats (id INT PRIMARY KEY)');

		$this->expectException(Devour\Migrations\MigrationException::class);
		$this->expectExceptionMessage('incomplete');

		try {
			Devour\Migrations\MigrationRunner::migrate($this->database);
		} finally {
			$this->assertSame(0, (int) $this->database->query('SELECT COUNT(*) FROM devour_migrations')->fetchColumn());
		}
	}
}
