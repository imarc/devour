<?php

use PHPUnit\Framework\TestCase;

final class MigrationRunnerTest extends TestCase
{
	public function testMigrationTypesAreAvailable()
	{
		$this->assertTrue(interface_exists(Devour\Migrations\Migration::class));
		$this->assertTrue(class_exists(Devour\Migrations\MigrationException::class));
		$this->assertTrue(class_exists(Devour\Migrations\MigrationRunner::class));
	}


	public function testBaselineMigrationUsesVersionNamespace()
	{
		$migration = new Devour\Migrations\Versions\Migration001();

		$this->assertSame(1, $migration->getId());
		$this->assertSame('Create Devour legacy schema', $migration->getDescription());
	}


	public function testRunnerRejectsNonPostgresConnections()
	{
		$database = new PDO('sqlite::memory:');

		$this->expectException(Devour\Migrations\MigrationException::class);
		$this->expectExceptionMessage('PostgreSQL');

		Devour\Migrations\MigrationRunner::migrate($database);
	}


	public function testReadinessRejectsNonPostgresConnections()
	{
		$database = new PDO('sqlite::memory:');

		$this->expectException(Devour\Migrations\MigrationException::class);
		$this->expectExceptionMessage('PostgreSQL');

		Devour\Migrations\MigrationRunner::assertReady($database);
	}


	public function testSynchronizerRejectsDatabaseWithoutMigrationReadiness()
	{
		$database = new PDO('sqlite::memory:');

		$this->expectException(Devour\Migrations\MigrationException::class);
		$this->expectExceptionMessage('PostgreSQL');

		new Devour\Synchronizer($database, $database);
	}


	public function testAnalyzerRejectsDatabaseWithoutMigrationReadiness()
	{
		$database = new PDO('sqlite::memory:');

		$this->expectException(Devour\Migrations\MigrationException::class);
		$this->expectExceptionMessage('PostgreSQL');

		new Devour\Analyzer($database);
	}


	public function testLegacySchemaCreationMethodsDirectOperatorsToMigrations()
	{
		$database = new PDO('sqlite::memory:');
		$synchronizer = new TestSynchronizer($database, $database);

		$this->expectException(Devour\Migrations\MigrationException::class);
		$this->expectExceptionMessage('MigrationRunner::migrate()');

		$synchronizer->createStatsTable();
	}
}
