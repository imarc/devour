<?php

namespace Devour\Migrations;

use PDO;

use ReflectionClass;
use Devour\Migrations\Versions\Migration001;

class MigrationRunner
{
	private const LOCK_KEY = 349412082173642345;

	private const LOCK_TIMEOUT_SECONDS = 30;


	public static function migrate(PDO $database): void
	{
		self::assertPostgres($database);
		self::assertNoTransaction($database);
		$schema = self::schema($database);
		$migrations = static::migrations();
		self::validateRegistry($migrations);
		$locked = FALSE;
		$failure = NULL;
		try {
			self::lock($database);
			$locked = TRUE;
			self::inTransaction($database, function() use ($database, $schema) {
				self::createLedger($database, $schema);
			});
			$applied = self::applied($database, $schema);
			self::validateApplied($migrations, $applied);

			foreach ($migrations as $migration) {
				if (isset($applied[$migration->getId()])) {
					continue;
				}

				self::inTransaction($database, function() use ($database, $schema, $migration) {
					$migration->up($database);
					self::record($database, $schema, $migration);
				});
			}
		} catch (\Throwable $exception) {
			$failure = $exception;
			throw $exception;
		} finally {
			if ($locked) {
				try {
					self::unlock($database);
				} catch (\Throwable $exception) {
					if (!$failure) {
						throw $exception;
					}
				}
			}
		}
	}


	public static function assertReady(PDO $database): void
	{
		self::assertPostgres($database);
		$schema = self::schema($database);
		$migrations = static::migrations();
		self::validateRegistry($migrations);

		if (!self::tableExists($database, $schema, 'devour_migrations')) {
			throw new MigrationException('Devour migrations are missing. Run MigrationRunner::migrate() during deployment.');
		}

		$applied = self::applied($database, $schema);
		self::validateApplied($migrations, $applied);

		foreach ($migrations as $migration) {
			if (!isset($applied[$migration->getId()])) {
				throw new MigrationException(sprintf('Devour migration %d is pending. Run MigrationRunner::migrate() during deployment.', $migration->getId()));
			}
		}
	}


	protected static function migrations(): array
	{
		return [new Migration001()];
	}


	private static function assertPostgres(PDO $database): void
	{
		if ($database->getAttribute(PDO::ATTR_DRIVER_NAME) !== 'pgsql') {
			throw new MigrationException('Devour migrations require a PostgreSQL PDO connection.');
		}
	}


	private static function assertNoTransaction(PDO $database): void
	{
		if ($database->inTransaction()) {
			throw new MigrationException('Devour migrations cannot run inside a caller-owned transaction.');
		}
	}


	private static function schema(PDO $database): string
	{
		$schema = $database->query('SELECT current_schema()')->fetchColumn();

		if (!$schema) {
			throw new MigrationException('Devour migrations require a current PostgreSQL schema.');
		}

		return $schema;
	}


	private static function validateRegistry(array $migrations): void
	{
		$previous = 0;

		foreach ($migrations as $migration) {
			if (!$migration instanceof Migration) {
				throw new MigrationException('Devour migration registry contains an invalid migration.');
			}

			$id = $migration->getId();
			if ($id <= $previous || trim($migration->getDescription()) === '') {
				throw new MigrationException('Devour migration registry must use non-empty descriptions and strictly ascending positive IDs.');
			}

			self::checksum($migration);
			$previous = $id;
		}
	}


	private static function lock(PDO $database): void
	{
		$deadline = microtime(TRUE) + self::LOCK_TIMEOUT_SECONDS;
		do {
			if ((bool) $database->query(sprintf('SELECT pg_try_advisory_lock(%d)', self::LOCK_KEY))->fetchColumn()) {
				return;
			}

			usleep(100000);
		} while (microtime(TRUE) < $deadline);

		throw new MigrationException('Timed out waiting for the Devour migration lock.');
	}


	private static function unlock(PDO $database): void
	{
		if (!(bool) $database->query(sprintf('SELECT pg_advisory_unlock(%d)', self::LOCK_KEY))->fetchColumn()) {
			throw new MigrationException('Failed releasing the Devour migration lock.');
		}
	}


	private static function inTransaction(PDO $database, callable $operation): void
	{
		$database->beginTransaction();
		try {
			$operation();
			$database->commit();
		} catch (\Throwable $exception) {
			if ($database->inTransaction()) {
				$database->rollBack();
			}

			throw $exception;
		}
	}


	private static function createLedger(PDO $database, string $schema): void
	{
		$database->exec(sprintf('CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY, checksum CHAR(64) NOT NULL, description TEXT NOT NULL, applied_at TIMESTAMP NOT NULL)', self::qualify($schema, 'devour_migrations')));
	}


	private static function tableExists(PDO $database, string $schema, string $table): bool
	{
		$statement = $database->prepare('SELECT to_regclass(:relation) IS NOT NULL');
		$statement->execute(['relation' => $schema . '.' . $table]);

		return (bool) $statement->fetchColumn();
	}


	private static function applied(PDO $database, string $schema): array
	{
		$rows = $database->query(sprintf('SELECT id, checksum FROM %s', self::qualify($schema, 'devour_migrations')))->fetchAll(PDO::FETCH_ASSOC);

		return array_column($rows, 'checksum', 'id');
	}


	private static function validateApplied(array $migrations, array $applied): void
	{
		$known = [];
		foreach ($migrations as $migration) {
			$known[$migration->getId()] = self::checksum($migration);
		}

		foreach ($applied as $id => $checksum) {
			if (!isset($known[$id])) {
				throw new MigrationException(sprintf('Devour database migration %d is newer than installed Devour.', $id));
			}

			if (!hash_equals($known[$id], trim($checksum))) {
				throw new MigrationException(sprintf('Devour migration %d checksum does not match; migration history changed.', $id));
			}
		}
	}


	private static function record(PDO $database, string $schema, Migration $migration): void
	{
		$statement = $database->prepare(sprintf('INSERT INTO %s (id, checksum, description, applied_at) VALUES (:id, :checksum, :description, CURRENT_TIMESTAMP)', self::qualify($schema, 'devour_migrations')));
		$statement->execute([
			'id' => $migration->getId(),
			'checksum' => self::checksum($migration),
			'description' => $migration->getDescription(),
		]);
	}


	private static function checksum(Migration $migration): string
	{
		$file = (new ReflectionClass($migration))->getFileName();
		$contents = $file ? file_get_contents($file) : FALSE;

		if ($contents === FALSE) {
			throw new MigrationException(sprintf('Cannot read source for Devour migration %d.', $migration->getId()));
		}

		return hash('sha256', $contents);
	}


	private static function qualify(string $schema, string $name): string
	{
		return sprintf('"%s"."%s"', str_replace('"', '""', $schema), str_replace('"', '""', $name));
	}
}
