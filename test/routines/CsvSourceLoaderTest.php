<?php

use PHPUnit\Framework\TestCase;

final class CsvSourceLoaderTest extends TestCase
{
	public function testMaterializeCsvIntoTemporaryTable()
	{
		$path = tempnam(sys_get_temp_dir(), 'devour_csv_');
		file_put_contents($path, "id,title\n1,Alpha\n2,Beta\n");

		$database = new PDO('sqlite::memory:');
		$mapping  = new Devour\Mapping('placeholder', 'events', 'id');
		$mapping
			->addField('id', 'csvsrc.id')
			->addField('title', 'csvsrc.title')
			->setCsvConfig([
				'path'   => $path,
				'header' => TRUE,
				'alias'  => 'csvsrc'
			]);

		$loader = new Devour\CsvSourceLoader();
		$table  = $loader->materialize($database, $mapping);

		$count = (int) $database->query("SELECT COUNT(*) FROM $table")->fetchColumn();
		$this->assertEquals(2, $count);

		$rows = $database->query("SELECT id, title FROM $table ORDER BY id ASC")->fetchAll(PDO::FETCH_ASSOC);
		$this->assertEquals('1', $rows[0]['id']);
		$this->assertEquals('Alpha', $rows[0]['title']);
		$this->assertEquals('2', $rows[1]['id']);
		$this->assertEquals('Beta', $rows[1]['title']);

		unlink($path);
	}
}
