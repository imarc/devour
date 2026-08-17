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


	/**
	 * A fresh install should look like what Postgres would build for a SERIAL id on this table, so
	 * a new society's schema is comparable to the deployed ones instead of carrying arbitrary
	 * sequence settings.
	 */
	public function testMigrateCreatesConventionallyNamedSequence(): void
	{
		Devour\Migrations\MigrationRunner::migrate($this->database);

		$sequence = $this->database->query("SELECT pg_get_serial_sequence('devour_stats', 'id')")->fetchColumn();
		$this->assertSame($this->schema . '.devour_stats_id_seq', $sequence);

		$settings = $this->database->query("SELECT seqstart, seqincrement FROM pg_sequence WHERE seqrelid = 'devour_stats_id_seq'::regclass")->fetch(PDO::FETCH_ASSOC);
		$this->assertSame('1', (string) $settings['seqstart']);
		$this->assertSame('1', (string) $settings['seqincrement']);

		$this->database->exec("INSERT INTO devour_stats (start_time) VALUES (CURRENT_TIMESTAMP)");
		$this->assertSame('1', (string) $this->database->query('SELECT id FROM devour_stats')->fetchColumn());
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


	/**
	 * Earlier Devour releases created the id sequence with an arbitrary START 100 INCREMENT 5.
	 * Anything built by those releases still has to baseline.
	 */
	public function testMigrateBaselinesSchemaFromEarlierRelease(): void
	{
		$this->createLegacyStats('devour_stats_id_seq', 'START 100 INCREMENT 5');
		$this->createLegacyUpdates();

		Devour\Migrations\MigrationRunner::migrate($this->database);

		$this->assertSame('1', (string) $this->database->query('SELECT id FROM devour_migrations')->fetchColumn());
	}


	/**
	 * Every deployed CPA society install carries a sequence named for the devour_stats_new table
	 * the current devour_stats was renamed from, starting at 1 and incrementing by 1.
	 */
	public function testMigrateBaselinesSequenceLeftBehindByLegacyRename(): void
	{
		$this->createLegacyStats('devour_stats_new_id_seq1', '');
		$this->createLegacyUpdates();

		Devour\Migrations\MigrationRunner::migrate($this->database);

		$this->assertSame('1', (string) $this->database->query('SELECT id FROM devour_migrations')->fetchColumn());
	}


	public function testMigrateBaselinesReorderedColumns(): void
	{
		$this->database->exec('CREATE SEQUENCE devour_stats_id_seq');
		$this->database->exec("CREATE TABLE devour_stats (id INT NOT NULL DEFAULT nextval('devour_stats_id_seq') PRIMARY KEY, start_time TIMESTAMP, scheduled_by TEXT, scheduled_time TIMESTAMP, end_time TIMESTAMP, tables TEXT, force BOOLEAN, log TEXT, ids TEXT)");
		$this->createLegacyUpdates();

		Devour\Migrations\MigrationRunner::migrate($this->database);

		$this->assertSame('1', (string) $this->database->query('SELECT id FROM devour_migrations')->fetchColumn());
	}


	public function testMigrateBaselinesEquivalentColumnTypes(): void
	{
		$this->database->exec('CREATE TABLE devour_stats (id BIGSERIAL PRIMARY KEY, start_time TIMESTAMP, scheduled_by VARCHAR(255), scheduled_time TIMESTAMP, end_time TIMESTAMP, tables TEXT, ids TEXT, force BOOLEAN, log TEXT)');
		$this->database->exec('CREATE TABLE devour_updates (target TEXT PRIMARY KEY, time TIMESTAMP)');

		Devour\Migrations\MigrationRunner::migrate($this->database);

		$this->assertSame('1', (string) $this->database->query('SELECT id FROM devour_migrations')->fetchColumn());
	}


	public function testBaselinedLegacySchemaAcceptsStatsInserts(): void
	{
		$this->createLegacyStats('devour_stats_new_id_seq1', '');
		$this->createLegacyUpdates();

		Devour\Migrations\MigrationRunner::migrate($this->database);

		$this->database->exec("INSERT INTO devour_stats (start_time, scheduled_by) VALUES (CURRENT_TIMESTAMP, 'test')");

		$this->assertSame(1, (int) $this->database->query('SELECT COUNT(*) FROM devour_stats')->fetchColumn());
		$this->assertNotNull($this->database->query('SELECT id FROM devour_stats ORDER BY id DESC LIMIT 1')->fetchColumn());
	}


	public function testMigrateRejectsIncompleteLegacySchemaWithoutRecordingMigration(): void
	{
		$this->database->exec('CREATE TABLE devour_stats (id INT PRIMARY KEY)');

		$this->expectException(Devour\Migrations\MigrationException::class);
		$this->expectExceptionMessage('Back up and delete or rename the existing Devour tables');

		try {
			Devour\Migrations\MigrationRunner::migrate($this->database);
		} finally {
			$this->assertSame(0, (int) $this->database->query('SELECT COUNT(*) FROM devour_migrations')->fetchColumn());
		}
	}


	public function testMigrateRejectsLegacyStatsMissingAColumn(): void
	{
		$this->database->exec('CREATE SEQUENCE devour_stats_id_seq');
		$this->database->exec("CREATE TABLE devour_stats (id INT NOT NULL DEFAULT nextval('devour_stats_id_seq') PRIMARY KEY, start_time TIMESTAMP, scheduled_by TEXT, scheduled_time TIMESTAMP, end_time TIMESTAMP, tables TEXT, ids TEXT, force BOOLEAN)");
		$this->createLegacyUpdates();

		$this->expectException(Devour\Migrations\MigrationException::class);
		$this->expectExceptionMessage('devour_stats is missing column log.');

		Devour\Migrations\MigrationRunner::migrate($this->database);
	}


	public function testMigrateRejectsLegacyStatsWithAnUnexpectedColumn(): void
	{
		$this->createLegacyStats('devour_stats_id_seq', '');
		$this->database->exec('ALTER TABLE devour_stats ADD COLUMN surprise TEXT');
		$this->createLegacyUpdates();

		$this->expectException(Devour\Migrations\MigrationException::class);
		$this->expectExceptionMessage('Unexpected column devour_stats.surprise.');

		Devour\Migrations\MigrationRunner::migrate($this->database);
	}


	public function testMigrateRejectsLegacyStatsWithAnIncompatibleColumnType(): void
	{
		$this->database->exec('CREATE SEQUENCE devour_stats_id_seq');
		$this->database->exec("CREATE TABLE devour_stats (id INT NOT NULL DEFAULT nextval('devour_stats_id_seq') PRIMARY KEY, start_time TIMESTAMP, scheduled_by TEXT, scheduled_time TIMESTAMP, end_time TIMESTAMP, tables TEXT, ids TEXT, force TEXT, log TEXT)");
		$this->createLegacyUpdates();

		$this->expectException(Devour\Migrations\MigrationException::class);
		$this->expectExceptionMessage('devour_stats.force is text; expected a boolean type.');

		Devour\Migrations\MigrationRunner::migrate($this->database);
	}


	public function testMigrateRejectsLegacyStatsWithoutAGeneratedId(): void
	{
		$this->database->exec('CREATE TABLE devour_stats (id INT NOT NULL PRIMARY KEY, start_time TIMESTAMP, scheduled_by TEXT, scheduled_time TIMESTAMP, end_time TIMESTAMP, tables TEXT, ids TEXT, force BOOLEAN, log TEXT)');
		$this->createLegacyUpdates();

		$this->expectException(Devour\Migrations\MigrationException::class);
		$this->expectExceptionMessage('devour_stats.id must default from a sequence or be an identity column.');

		Devour\Migrations\MigrationRunner::migrate($this->database);
	}


	public function testMigrateRejectsLegacyStatsWithTheWrongPrimaryKey(): void
	{
		$this->database->exec('CREATE SEQUENCE devour_stats_id_seq');
		$this->database->exec("CREATE TABLE devour_stats (id INT NOT NULL DEFAULT nextval('devour_stats_id_seq'), start_time TIMESTAMP, scheduled_by TEXT, scheduled_time TIMESTAMP, end_time TIMESTAMP, tables TEXT, ids TEXT, force BOOLEAN, log TEXT, PRIMARY KEY (start_time))");
		$this->createLegacyUpdates();

		$this->expectException(Devour\Migrations\MigrationException::class);
		$this->expectExceptionMessage('devour_stats primary key must be id.');

		Devour\Migrations\MigrationRunner::migrate($this->database);
	}


	/**
	 * A rewritten migration history is a deployment problem, so it fails the deployment.
	 */
	public function testMigrateRejectsAppliedMigrationWhoseSourceChanged(): void
	{
		Devour\Migrations\MigrationRunner::migrate($this->database);
		$this->rewriteAppliedChecksum();

		$this->expectException(Devour\Migrations\MigrationException::class);
		$this->expectExceptionMessage('checksum does not match');

		Devour\Migrations\MigrationRunner::migrate($this->database);
	}


	/**
	 * Runtime does not police it, though.  The schema is present and correct either way, and
	 * failing here would take every consumer down instead of failing one deployment.
	 */
	public function testReadinessToleratesAppliedMigrationWhoseSourceChanged(): void
	{
		Devour\Migrations\MigrationRunner::migrate($this->database);
		$this->rewriteAppliedChecksum();

		Devour\Migrations\MigrationRunner::assertReady($this->database);

		$this->assertInstanceOf(Devour\Synchronizer::class, new Devour\Synchronizer($this->database, $this->database));
		$this->assertInstanceOf(Devour\Analyzer::class, new Devour\Analyzer($this->database));
	}


	/**
	 * Version skew still fails everywhere: a database migrated by a newer Devour than the one
	 * installed is a real runtime hazard, not just a deployment one.
	 */
	public function testReadinessRejectsMigrationFromNewerDevour(): void
	{
		Devour\Migrations\MigrationRunner::migrate($this->database);
		$this->database->exec("INSERT INTO devour_migrations (id, checksum, description, applied_at) VALUES (99, repeat('0', 64), 'From the future', CURRENT_TIMESTAMP)");

		$this->expectException(Devour\Migrations\MigrationException::class);
		$this->expectExceptionMessage('newer than installed Devour');

		Devour\Migrations\MigrationRunner::assertReady($this->database);
	}


	public function testReadinessRejectsPendingMigration(): void
	{
		Devour\Migrations\MigrationRunner::migrate($this->database);
		$this->database->exec('DELETE FROM devour_migrations WHERE id = 1');

		$this->expectException(Devour\Migrations\MigrationException::class);
		$this->expectExceptionMessage('Devour migration 1 is pending');

		Devour\Migrations\MigrationRunner::assertReady($this->database);
	}


	private function rewriteAppliedChecksum(): void
	{
		$this->database->exec("UPDATE devour_migrations SET checksum = repeat('a', 64) WHERE id = 1");
	}


	private function createLegacyStats(string $sequence, string $options): void
	{
		$this->database->exec(trim(sprintf('CREATE SEQUENCE %s %s', $sequence, $options)));
		$this->database->exec(sprintf("CREATE TABLE devour_stats (id INT NOT NULL DEFAULT nextval('%s') PRIMARY KEY, start_time TIMESTAMP, scheduled_by TEXT, scheduled_time TIMESTAMP, end_time TIMESTAMP, tables TEXT, ids TEXT, force BOOLEAN, log TEXT)", $sequence));
		$this->database->exec(sprintf('ALTER SEQUENCE %s OWNED BY devour_stats.id', $sequence));
	}


	private function createLegacyUpdates(): void
	{
		$this->database->exec('CREATE TABLE devour_updates (target VARCHAR(255) PRIMARY KEY, time TIMESTAMP)');
	}
}
