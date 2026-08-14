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

Devour manages its PostgreSQL tables through explicit, forward-only migrations. Run migrations during deployment before constructing `Synchronizer`, `Importer`, or `Analyzer`:

```php
Devour\Migrations\MigrationRunner::migrate($destination_database);
```

Devour never changes its schema during normal runtime. Construction throws `Devour\Migrations\MigrationException` when migrations are missing, pending, altered, or newer than the installed library. Fix the deployment migration step; do not modify `devour_migrations` manually.

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
