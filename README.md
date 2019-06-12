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
