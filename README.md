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

## CSV Source Imports

Use `Devour\Importer` for CSV workflows. It extends `Synchronizer`, uses a single database connection for both source and destination, and stages CSV data in the destination database.

```php
$sync = new Devour\Importer($database);

$mapping = new Devour\Mapping('placeholder', 'events', 'id');

$mapping
	->setCsvConfig([
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
$sync->run(['events']);
```

Example with explicit `columns` definitions:

```php
$mapping->setCsvConfig([
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
	persistent = true	; !!!IMPORTANT!!! without this, any rows now in the csv file will get deleted from the target table

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

Notes:

- CSV data is materialized into a temporary staging table on the destination database before synchronization.
- Existing database-to-database syncing remains in `Synchronizer`.
- You can optionally pass `columns` in `setCsvConfig()` to control temporary table column definitions.
- CSV mapping joins execute on the destination database, so join targets must be destination-accessible tables.
- IMPORTANT: set CSV mappings as persistent (`setPersistent(true)` in PHP or `persistent = true` in `.jin`) if you need to preserve existing destination rows not present in the CSV.
- If `persistent` is not set, normal sync delete behavior can remove destination rows that do not appear in the current CSV import.
