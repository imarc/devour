<?php

namespace Devour\Migrations\Versions;

use PDO;
use Devour\Migrations\Migration;
use Devour\Migrations\MigrationException;

class Migration001 implements Migration
{
	/**
	 * Postgres types accepted for each logical column kind.
	 *
	 * Baselining validates that an existing schema is functionally compatible with Devour, not
	 * that it is byte-identical to what up() creates.  Legacy Devour tables predate this migration
	 * and were built by hand, by earlier Devour releases, and by table renames, so equivalent
	 * types have to be accepted.
	 */
	private const TYPE_KINDS = [
		'integer'   => ['smallint', 'integer', 'bigint'],
		'text'      => ['text', 'character varying', 'character'],
		'timestamp' => ['timestamp without time zone', 'timestamp with time zone'],
		'boolean'   => ['boolean'],
	];

	/**
	 * The nine columns of the pre-migration schema.
	 *
	 * Do not add columns introduced by later migrations here.  This list validates a legacy schema
	 * during baselining, which only ever happens before those migrations run, and assertColumns()
	 * rejects anything unexpected — so adding Migration002's columns would fail every legacy
	 * baseline.
	 */
	private const STATS_COLUMNS = [
		'id'             => 'integer',
		'start_time'     => 'timestamp',
		'scheduled_by'   => 'text',
		'scheduled_time' => 'timestamp',
		'end_time'       => 'timestamp',
		'tables'         => 'text',
		'ids'            => 'text',
		'force'          => 'boolean',
		'log'            => 'text',
	];

	private const UPDATES_COLUMNS = [
		'target' => 'text',
		'time'   => 'timestamp',
	];


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
			$database->exec(sprintf('CREATE SEQUENCE %s START 1 INCREMENT 1', $sequence));
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
		$this->assertColumns($database, $schema, 'devour_stats', self::STATS_COLUMNS);
		$this->assertColumns($database, $schema, 'devour_updates', self::UPDATES_COLUMNS);

		$this->assertPrimaryKey($database, $schema, 'devour_stats', 'id');
		$this->assertPrimaryKey($database, $schema, 'devour_updates', 'target');

		$this->assertGeneratedId($database, $schema);
	}


	/**
	 * Assert a table has exactly the expected columns, each of a compatible type.
	 *
	 * Column order is not checked: it varies across legacy installs and has no bearing on the
	 * queries Devour runs.  Nullability is not checked either; the primary key is covered by the
	 * primary key assertion, and Devour always writes every remaining column, so an install that
	 * tightened one to NOT NULL still works.
	 *
	 * @param array<string, string> $expected Column name mapped to its key in self::TYPE_KINDS
	 */
	private function assertColumns(PDO $database, string $schema, string $table, array $expected): void
	{
		$actual = $this->columns($database, $schema, $table);

		$missing = array_diff(array_keys($expected), array_keys($actual));
		if ($missing) {
			throw $this->unsupportedLegacySchema(sprintf('%s is missing column %s.', $table, reset($missing)));
		}

		$extra = array_diff(array_keys($actual), array_keys($expected));
		if ($extra) {
			throw $this->unsupportedLegacySchema(sprintf('Unexpected column %s.%s.', $table, reset($extra)));
		}

		foreach ($expected as $column => $kind) {
			if (!in_array($actual[$column]['type'], self::TYPE_KINDS[$kind], TRUE)) {
				throw $this->unsupportedLegacySchema(sprintf(
					'%s.%s is %s; expected a %s type.',
					$table,
					$column,
					$actual[$column]['type'],
					$kind
				));
			}
		}
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


	/**
	 * Assert devour_stats.id populates itself.
	 *
	 * Devour inserts stats rows without an id and reads the result back through lastInsertId(),
	 * which resolves to lastval(), so the only requirement is that the column draws from some
	 * sequence.  The sequence's name, start, increment, and ownership are incidental: legacy
	 * installs carry sequences left behind by a devour_stats_new rename, and lastval() does not
	 * care what they are called.
	 */
	private function assertGeneratedId(PDO $database, string $schema): void
	{
		$columns = $this->columns($database, $schema, 'devour_stats');

		if (!$columns['id']['generated']) {
			throw $this->unsupportedLegacySchema('devour_stats.id must default from a sequence or be an identity column.');
		}
	}


	/**
	 * @return array<string, array{type: string, not_null: bool, generated: bool}>
	 */
	private function columns(PDO $database, string $schema, string $table): array
	{
		$statement = $database->prepare("SELECT a.attname, format_type(a.atttypid, NULL) AS type, a.attnotnull::int AS not_null, (a.attidentity <> '')::int AS is_identity, pg_get_expr(d.adbin, d.adrelid) AS default_expr FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum WHERE n.nspname = :schema AND c.relname = :table AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum");
		$statement->execute(['schema' => $schema, 'table' => $table]);

		$columns = [];

		foreach ($statement->fetchAll(PDO::FETCH_ASSOC) as $row) {
			$columns[$row['attname']] = [
				'type'      => $row['type'],
				'not_null'  => (int) $row['not_null'] === 1,
				'generated' => (int) $row['is_identity'] === 1 || stripos((string) $row['default_expr'], 'nextval(') !== FALSE,
			];
		}

		return $columns;
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
