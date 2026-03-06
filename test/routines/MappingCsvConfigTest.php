<?php

use PHPUnit\Framework\TestCase;

final class MappingCsvConfigTest extends TestCase
{
	public function testCsvSourceDisabledByDefault()
	{
		$mapping = new Devour\Mapping('source_table', 'destination_table', 'id');

		$this->assertFalse($mapping->isCsvSource());
		$this->assertNull($mapping->getCsvConfig());
	}


	public function testCsvSourceConfig()
	{
		$mapping = new Devour\Mapping('source_table', 'destination_table', 'id');

		$mapping->setCsvConfig([
			'path' => '/tmp/import.csv'
		]);

		$this->assertTrue($mapping->isCsvSource());
		$this->assertEquals('/tmp/import.csv', $mapping->getCsvConfig()['path']);
		$this->assertEquals('csvsrc', $mapping->getCsvConfig()['alias']);
	}
}
