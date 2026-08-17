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

### Upgrading To 5.0.0

5.0.0 adds `Migration002` (`canceled_time`, `canceled_by`, `heartbeat`, `max_gap`). The API additions are backwards compatible, but `assertReady()` refuses any database with a pending migration — so `Synchronizer`, `Importer`, `Analyzer`, and `Supervisor` all fail to construct until migrations run. Upgrading the package alone takes syncing down.

Per site: update the package, run `MigrationRunner::migrate()`, and only then start sync workers.

Rolling back is **not** symmetrical. `validateAppliedVersions()` treats a recorded migration the installed library does not know about as code and schema drifting out of step, so a database carrying migration 2 refuses to work with a 4.x library. To roll back, delete the row with `id = 2` from `devour_migrations`; the added columns are harmless left in place.

### Runtime Failures

Devour never changes schema during normal runtime. Construction throws `Devour\Migrations\MigrationException` when migrations are missing, pending, or newer than the installed library. Treat this as a deployment failure: run migrations with the matching library release, then start application processes.

`migrate()` validates more than construction does. It also verifies that every applied migration's source still hashes to the checksum recorded in `devour_migrations`, and fails with `Devour migration N checksum does not match` when it does not. That check is deliberately confined to deployment: an edited migration file means the recorded history no longer describes the database, which is actionable while deploying and has no runtime consequence. Enforcing it at construction time would instead take every running consumer down at once, long after the deployment that caused it.

If you hit a checksum failure, either restore the migration file to its applied form, or — if the edit was intentional and the schema it produces is unchanged — reconcile `devour_migrations` before deploying again.

```php
try {
	MigrationRunner::migrate($database);
} catch (Devour\Migrations\MigrationException $exception) {
	// Stop deployment; do not start Devour workers.
	throw $exception;
}
```

## Cancelling A Stuck Sync

`Supervisor` reads and cancels runs. Like `Analyzer`, it needs only the destination connection — no mappings, no source database.

```php
use Devour\Run;
use Devour\Supervisor;

$supervisor = new Supervisor($database);

foreach ($supervisor->findRunning() as $run) {
	echo $run->getSummary() . PHP_EOL;

	if ($run->getConfidence() === Run::STUCK) {
		$cancelled = $supervisor->cancel($run->getId(), 'ops@example.com');

		// $cancelled->getLog() holds the run's log, preserved on the row.
	}
}
```

A cancellation sets `canceled_time` and leaves `end_time` NULL, so cancelled runs stay out of the duration history that `getSyncInterval()` derives from `MAX(end_time - start_time)`. A run that sat dead for three days therefore cannot redefine what "normal" looks like.

Cancellation is cooperative. `Synchronizer` guards the UPDATE it already issues on every log line with `AND canceled_time IS NULL`, so a still-running worker sees a zero row count, throws `Devour\CanceledException`, and stops at its next log line without stamping `end_time`.

### Staleness

`getConfidence()` returns `Run::HEALTHY`, `Run::SUSPECT`, `Run::STUCK`, or `Run::UNKNOWN`. It compares how long a run has been silent against the largest gap between log lines that a completed run of the same shape has ever recorded, tracked live in `max_gap`:

```
silence  = now − COALESCE(heartbeat, start_time)
baseline = MAX(max_gap) over completed runs of the same context
```

Runs are bucketed by context — a full sync, a table-limited sync, and a single-record sync have legitimately different log cadences — matching how `getSyncInterval()` buckets durations. Silence under `Run::SILENCE_FLOOR` (300s) is always healthy, so a site whose gaps are measured in seconds does not flap.

The verdict is **advisory**. `cancel()` will stop any running sync regardless of what `getConfidence()` says; the signal exists to inform an operator, not to gate them.

`max_gap` cannot be contaminated by the stalls it detects: it only ever grows from an interval that was actually observed between two written lines, and a worker that dies writes nothing further.

## Syncing A Subset

`run()` accepts specific ids, syncing only those rows rather than whole tables. Two mapping settings control what comes along with them.

### `require` — rows this table depends on

Tables that must be populated first, typically to satisfy foreign keys. Synced **before** this mapping. Each entry must use the object form:

```jin
require = [
	{ "table": "event_levels", "source": "sycod.code", "key": "level" }
]
```

- **`source`** — a column in the dependency's own `FROM` clause
- **`key`** — the field alias on *this* mapping that it joins against

A bare table name (`"event_levels"`) also parses, but gives Devour no way to work out which rows a subset needs — so it falls back to syncing that **entire table**. One entry in bare form turns a single-record sync into a full sync of that dependency, which is rarely what a subset sync is for. Use the object form unless you genuinely want the whole table every time.

Only declare a dependency the mapping actually references. If there is no field pointing at it, there is no foreign key to satisfy and nothing to join on.

### `adjunct` — rows that depend on this table

Child records, synced **after** this mapping, and only when a subset is being synced — a full sync covers them anyway:

```jin
adjunct = {
	"event_sessions": { "source": "event.control" }
}
```

- **`source`** — the column in the adjunct's own source that points back at this table's key

Without adjuncts, a subset sync updates the parent rows alone and leaves their children stale until the next full sync.

Adjuncts do not chain: an adjunct's own adjuncts are not followed, so a subset sync stays bounded.

### Estimating How Long A Sync Will Take

`getSyncInterval($context, $mode)` reports how long runs of a given shape take, and `getCompletionTime($context)` turns that into an expected finish time for the run in flight. Runs are bucketed so a sync is only compared against its own shape:

| Context | Meaning |
|---|---|
| `individual` | specific tables, specific ids |
| `limited` | specific tables, all ids |
| `NULL` | a full sync |

An **empty JSON list counts as unspecified**. `schedule()` stores `json_encode($mappings)`, so a full sync scheduled through a UI arrives as the string `'[]'` rather than `NULL`; treating that as "specific tables" files hours-long full syncs into the bucket a single-table sync is estimated from.

`'high'` mode returns the **90th percentile of the most recent `Synchronizer::INTERVAL_SAMPLE` runs**, not the maximum over all history. A maximum lets one anomalous run set the ceiling permanently and counts runs that barely started — a full sync that died after five seconds otherwise sits in the same history as the two-hour ones. A percentile absorbs both without needing a threshold for what counts as a real run.

Both methods return `NULL` when a context has no completed runs yet. Handle that rather than coercing it to a number: `start_time + 0` renders as a completion estimate that has already passed.

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
