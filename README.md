# Devour Database Synchronization Library

```php

$sync  = new Devour\Synchronizer();
$table = new Devour\Mapping('events');

$table->addKey('id')
$table->setSource('evmas');
$table->setParam('tracker_limit', date('Y-m-d', strtotime('-1 year')));
$table->addJoin('vendor', 'firm', ['vendor = firm.id']);
$table->addField('id', 'evmas.control');
$table->addFilter('highlights', 'evmas.majordesc');
$table->addWhere("exclude_from_catalog != 'Y'");
$table->addWhere("end_date >= '{{ tracker_limit }}'");


$sync->addTable($table);
$sync->run('events')
```

## Database Migrations

Devour manages its PostgreSQL tables through explicit, forward-only migrations. Migrations create and update Devour-owned tables such as `devour_stats`, `devour_updates`, and `devour_migrations` as the library evolves.

Run migrations during every deployment, before constructing `Synchronizer`, `Importer`, or `Analyzer` and before starting sync workers. `migrate()` is safe to call repeatedly: it records each successful migration in `devour_migrations` and runs only pending versions.

```php
use Devour\Migrations\MigrationRunner;

$database = new PDO($dsn, $username, $password, [
	PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
]);

MigrationRunner::migrate($database);

// Safe only after migrations complete.
$sync = new Devour\Synchronizer($database, $database);
```

### Requirements

- Devour migrations require PostgreSQL.
- Run deploy and runtime connections against the same PostgreSQL schema.
- Do not call `migrate()` inside an application-managed transaction.
- Migration definitions are forward-only. Never alter applied migration files or manually edit `devour_migrations`.

### First Deploy And Existing Installations

On a fresh database, the first migration creates Devour's current schema. On an existing installation, it validates the legacy `devour_stats` and `devour_updates` schema before recording the baseline migration. Baselining never writes to an existing schema: it either records the migration or fails.

Validation checks that the existing schema is *functionally* compatible with Devour, not that it is identical to what a fresh install creates. Legacy tables were built by hand, by earlier Devour releases, and by table renames, so the baseline accepts any equivalent shape:

- Both tables must have exactly the expected column names, in any order.
- Column types must be compatible, not exact: any integer width for `devour_stats.id`, `text` or `varchar` for text columns, `timestamp` with or without time zone for timestamps.
- `devour_stats.id` must be the primary key and populate itself, from either a sequence default or an identity column. The sequence's name, start, increment, and ownership do not matter — Devour reads new ids back through `lastInsertId()`, which resolves to `lastval()`.
- `devour_updates.target` must be the primary key.

If the existing schema is genuinely incompatible — a missing column, an unexpected column, an incompatible type, the wrong primary key, or an `id` that does not populate itself — deployment fails without changing it. Back up and delete or rename the Devour tables, then rerun the migration step to create fresh tables.

### Runtime Failures

Devour never changes schema during normal runtime. Construction throws `Devour\Migrations\MigrationException` when migrations are missing, pending, altered, or newer than the installed library. Treat this as a deployment failure: run migrations with the matching library release, then start application processes.

```php
try {
	MigrationRunner::migrate($database);
} catch (Devour\Migrations\MigrationException $exception) {
	// Stop deployment; do not start Devour workers.
	throw $exception;
}
```

## CSV Source Imports

Use `Devour\Importer` for file workflows. It extends `Synchronizer`, uses a single database connection for both source and destination, and stages file data in the destination database through a pluggable file driver.

```php
$sync = new Devour\Importer($database);

$mapping = new Devour\Mapping('placeholder', 'events', 'id');

$mapping
	->setFileConfig('csv', [
		'path'      => '/path/to/events.csv',
		'header'    => true,
		'delimiter' => ',',
		'enclosure' => '"',
		'escape'    => '\\',
		'alias'     => 'csvsrc'
	])
	->addField('id', 'csvsrc.id')
	->addField('title', 'csvsrc.title')
	->addField('start_date', 'csvsrc.start_date')
	->addWhere('csvsrc.id IS NOT NULL')
;

$sync->addMapping($mapping);
$sync->runWithDriver(new Devour\CsvDriver(), ['events']);
```

Example with explicit `columns` definitions:

```php
$mapping->setFileConfig('csv', [
	'path'      => '/path/to/events.csv',
	'header'    => true,
	'alias'     => 'csvsrc',
	'columns'   => [
		'id'         => 'integer',
		'title'      => 'text',
		'start_date' => 'date',
		'price'      => 'numeric(10,2)'
	]
]);
```

Example `.jin` mapping for CSV imports (recommended `persistent = true`):

```ini
[devour.map]
	target = events
	key    = id
	source = csvsrc
	persistent = true

	fields = {
		"id"         : "csvsrc.id",
		"title"      : "csvsrc.title",
		"start_date" : "csvsrc.start_date"
	}

	[&.csv]
		path      = env('EVENTS_CSV', '/path/to/events.csv')
		header    = true
		delimiter = ","
		enclosure = "\""
		escape    = "\\"
		alias     = "csvsrc"
```

Custom file drivers can implement `Devour\FileDriver` and be passed to `Devour\Importer::runWithDriver()` in place of `Devour\CsvDriver`.

Notes:

- CSV data is materialized into a temporary staging table on the destination database before synchronization.
- `Importer` accepts a generic file driver at runtime via `runWithDriver(FileDriver $driver, ...)`; `CsvDriver` is the default implementation for CSV imports.
- `Mapping` is file-driver agnostic; provide file settings with `setFileConfig('<type>', [...])`.
- For CSV imports, you can optionally pass `columns` in `setFileConfig('csv', ...)` to control temporary table column definitions.
- CSV mapping joins execute on the destination database, so join targets must be destination-accessible tables.
- IMPORTANT: set CSV mappings as persistent (`setPersistent(true)` in PHP or `persistent = true` in `.jin`) if you need to preserve existing destination rows not present in the CSV.
	- If `persistent` is not set, normal sync delete behavior can remove destination rows that do not appear in the current CSV import.
