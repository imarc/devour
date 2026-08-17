<?php

namespace Devour\Migrations\Versions;

use PDO;
use Devour\Migrations\Migration;
use Devour\Migrations\MigrationException;

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
