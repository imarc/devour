<?php

namespace Devour\Migrations\Versions;

use PDO;
use Devour\Migrations\Migration;
use Devour\Migrations\MigrationException;

class Migration001 implements Migration
{
	public function getId(): int
	{
		return 1;
	}


	public function getDescription(): string
	{
		return 'Create Devour legacy schema';
	}


	public function up(PDO $database): void
	{
		$schema = $database->query('SELECT current_schema()')->fetchColumn();
		if (!$schema) {
			throw new MigrationException('Devour migrations require a current PostgreSQL schema.');
		}

		$stats   = $this->qualify($schema, 'devour_stats');
		$updates = $this->qualify($schema, 'devour_updates');
		$sequence = $this->qualify($schema, 'devour_stats_id_seq');

		$has_stats   = $this->relationExists($database, $schema, 'devour_stats');
		$has_updates = $this->relationExists($database, $schema, 'devour_updates');

		if (!$has_stats && !$has_updates) {
			$database->exec(sprintf('CREATE SEQUENCE %s START 100 INCREMENT 5', $sequence));
			$database->exec(sprintf(
				'CREATE TABLE %s (id INT NOT NULL DEFAULT nextval(%s) PRIMARY KEY, start_time TIMESTAMP, scheduled_by TEXT, scheduled_time TIMESTAMP, end_time TIMESTAMP, tables TEXT, ids TEXT, force BOOLEAN, log TEXT)',
				$stats,
				$database->quote($schema . '.devour_stats_id_seq')
			));
			$database->exec(sprintf('ALTER SEQUENCE %s OWNED BY %s.id', $sequence, $stats));
			$database->exec(sprintf('CREATE TABLE %s (target VARCHAR(255) PRIMARY KEY, time TIMESTAMP)', $updates));

			return;
		}

		if (!$has_stats || !$has_updates) {
			throw $this->unsupportedLegacySchema('Expected both devour_stats and devour_updates.');
		}

		$this->validateLegacySchema($database, $schema);
	}


	private function relationExists(PDO $database, string $schema, string $relation): bool
	{
		$statement = $database->prepare('SELECT to_regclass(:relation) IS NOT NULL');
		$statement->execute(['relation' => $schema . '.' . $relation]);

		return (bool) $statement->fetchColumn();
	}


	private function validateLegacySchema(PDO $database, string $schema): void
	{
		$expected = [
			'devour_stats' => [
				'id' => ['integer', TRUE],
				'start_time' => ['timestamp without time zone', FALSE],
				'scheduled_by' => ['text', FALSE],
				'scheduled_time' => ['timestamp without time zone', FALSE],
				'end_time' => ['timestamp without time zone', FALSE],
				'tables' => ['text', FALSE],
				'ids' => ['text', FALSE],
				'force' => ['boolean', FALSE],
				'log' => ['text', FALSE],
			],
			'devour_updates' => [
				'target' => ['character varying(255)', TRUE],
				'time' => ['timestamp without time zone', FALSE],
			],
		];

		foreach ($expected as $table => $columns) {
			$statement = $database->prepare('SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS type, a.attnotnull FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = :schema AND c.relname = :table AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum');
			$statement->execute(['schema' => $schema, 'table' => $table]);
			$actual = $statement->fetchAll(PDO::FETCH_ASSOC);

			if (count($actual) !== count($columns)) {
				throw $this->unsupportedLegacySchema(sprintf('%s has an unexpected column set.', $table));
			}

			foreach ($actual as $column) {
				if (!isset($columns[$column['attname']])) {
					throw $this->unsupportedLegacySchema(sprintf('Unexpected column %s.%s.', $table, $column['attname']));
				}

				[$type, $not_null] = $columns[$column['attname']];
				if ($column['type'] !== $type || (bool) $column['attnotnull'] !== $not_null) {
					throw $this->unsupportedLegacySchema(sprintf('Invalid definition for %s.%s.', $table, $column['attname']));
				}
			}

			$this->assertPrimaryKey($database, $schema, $table, array_key_first($columns));
		}

		$this->assertSequence($database, $schema);
	}


	private function assertPrimaryKey(PDO $database, string $schema, string $table, string $column): void
	{
		$statement = $database->prepare('SELECT array_agg(a.attname ORDER BY k.ordinality) FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace JOIN unnest(c.conkey) WITH ORDINALITY k(attnum, ordinality) ON TRUE JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum WHERE c.contype = :type AND n.nspname = :schema AND t.relname = :table');
		$statement->execute(['type' => 'p', 'schema' => $schema, 'table' => $table]);
		$result = $statement->fetchColumn();

		if ($result !== '{' . $column . '}') {
			throw $this->unsupportedLegacySchema(sprintf('%s primary key must be %s.', $table, $column));
		}
	}


	private function assertSequence(PDO $database, string $schema): void
	{
		$statement = $database->prepare("SELECT s.seqstart, s.seqincrement, pg_get_serial_sequence(format('%I.%I', :schema, 'devour_stats'), 'id'), pg_get_expr(d.adbin, d.adrelid), EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.refclassid = 'pg_class'::regclass AND d.refobjid = t.oid AND d.refobjsubid = a.attnum AND d.deptype = 'a') FROM pg_sequence s JOIN pg_class c ON c.oid = s.seqrelid JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_class t ON t.oid = to_regclass(format('%I.%I', :schema, 'devour_stats')) JOIN pg_attribute a ON a.attrelid = t.oid AND a.attname = 'id' LEFT JOIN pg_attrdef d ON d.adrelid = t.oid AND d.adnum = a.attnum WHERE n.nspname = :schema AND c.relname = 'devour_stats_id_seq'");
		$statement->execute(['schema' => $schema]);
		$result = $statement->fetch(PDO::FETCH_NUM);

		if (!$result || (int) $result[0] !== 100 || (int) $result[1] !== 5 || $result[2] !== $schema . '.devour_stats_id_seq' || strpos($result[3], "devour_stats_id_seq'::regclass)") === FALSE || !(bool) $result[4]) {
			throw $this->unsupportedLegacySchema('Invalid devour_stats_id_seq configuration.');
		}
	}


	private function unsupportedLegacySchema(string $reason): MigrationException
	{
		return new MigrationException(sprintf(
			'Devour migration cannot baseline the existing schema: %s Back up and delete or rename the existing Devour tables, then rerun MigrationRunner::migrate() to create fresh Devour tables.',
			$reason
		));
	}


	private function qualify(string $schema, string $name): string
	{
		return sprintf('"%s"."%s"', str_replace('"', '""', $schema), str_replace('"', '""', $name));
	}
}
