<?php

namespace Devour\Migrations\Versions;

use PDO;
use Devour\Migrations\Migration;
use Devour\Migrations\MigrationException;

/**
 * Separates a run's errors and its per-table figures out of the log prose.
 *
 * The log was doing three jobs at once: human-readable progress, the error report, and the source
 * Analyzer parsed statistics back out of.  That coupling is why the log could not be shortened —
 * dropping progress lines took the statistics with them, and errors stayed buried among them.
 */
class Migration003 implements Migration
{
	private const COLUMNS = [
		'error'       => 'TEXT',
		'table_stats' => 'TEXT',
	];


	public function getId(): int
	{
		return 3;
	}


	public function getDescription(): string
	{
		return 'Add error and per-table statistic columns to devour_stats';
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
