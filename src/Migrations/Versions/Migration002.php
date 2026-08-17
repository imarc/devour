<?php

namespace Devour\Migrations\Versions;

use PDO;
use Devour\Migrations\Migration;
use Devour\Migrations\MigrationException;

/**
 * Adds cancellation and liveness tracking to devour_stats.
 *
 * Note for whoever adds Migration003: do not touch an already-released migration's source, not even
 * to add a comment.  migrate() checksums each migration file against what was recorded in
 * devour_migrations when it was applied, so any edit — however cosmetic — makes every installation
 * that already ran it refuse to migrate.
 *
 * In particular, Migration001::STATS_COLUMNS deliberately lists only the nine original columns and
 * must stay that way.  It validates a legacy schema during baselining, which only happens before
 * this migration runs, and its assertColumns() rejects anything unexpected — so adding these
 * columns there would fail every legacy baseline as well as breaking the checksum.
 */
class Migration002 implements Migration
{
	/**
	 * Columns added to devour_stats, in the order they are appended.
	 *
	 * All are nullable with no default: rows written before this migration keep meaning what they
	 * meant, and Run treats a NULL heartbeat as "measure silence from start_time instead".
	 */
	private const COLUMNS = [
		'canceled_time' => 'TIMESTAMP',
		'canceled_by'   => 'TEXT',
		'heartbeat'     => 'TIMESTAMP',
		'max_gap'       => 'INTEGER',
	];


	public function getId(): int
	{
		return 2;
	}


	public function getDescription(): string
	{
		return 'Add cancellation and heartbeat tracking to devour_stats';
	}


	public function up(PDO $database): void
	{
		$schema = $database->query('SELECT current_schema()')->fetchColumn();

		if (!$schema) {
			throw new MigrationException('Devour migrations require a current PostgreSQL schema.');
		}

		$stats = sprintf(
			'"%s"."%s"',
			str_replace('"', '""', $schema),
			'devour_stats'
		);

		foreach (self::COLUMNS as $column => $type) {
			$database->exec(sprintf(
				'ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s',
				$stats,
				$column,
				$type
			));
		}
	}
}
