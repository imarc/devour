<?php

use PHPUnit\Framework\TestCase;

final class MappingCsvConfigTest extends TestCase
{
	public function testCsvSourceDisabledByDefault()
	{
		$mapping = new Devour\Mapping('source_table', 'destination_table', 'id');

		$this->assertFalse($mapping->isFileSource('csv'));
		$this->assertNull($mapping->getFileConfig('csv'));
	}


	public function testCsvSourceConfig()
	{
		$mapping = new Devour\Mapping('source_table', 'destination_table', 'id');

		$mapping->setFileConfig('csv', [
			'path' => '/tmp/import.csv'
		]);

		$this->assertTrue($mapping->isFileSource('csv'));
		$this->assertTrue($mapping->isFileSource());
		$this->assertEquals('csv', $mapping->getFileType());
		$this->assertEquals('/tmp/import.csv', $mapping->getFileConfig('csv')['path']);
	}


	public function testGenericFileConfigAccessors()
	{
		$mapping = new Devour\Mapping('source_table', 'destination_table', 'id');

		$mapping->setFileConfig('json', [
			'path' => '/tmp/import.json',
			'alias' => 'jsonsrc'
		]);

		$this->assertTrue($mapping->isFileSource());
		$this->assertFalse($mapping->isFileSource('csv'));
		$this->assertEquals('json', $mapping->getFileType());
		$this->assertEquals('/tmp/import.json', $mapping->getFileConfig()['path']);
		$this->assertEquals('jsonsrc', $mapping->getFileConfig('json')['alias']);
		$this->assertNull($mapping->getFileConfig('csv'));
	}
}
