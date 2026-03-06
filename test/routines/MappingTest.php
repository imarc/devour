<?php

use PHPUnit\Framework\TestCase;

final class MappingTest extends TestCase
{
	protected $parser;

	protected $config;

	protected $mapping;

	protected function setUp(): void
	{
		$this->parser = new Dotink\Jin\Parser();
		$this->config = $this->parser->parse(file_get_contents(__DIR__ . '/../config/Example.jin'));
		$this->mapping  = new Devour\Mapping(
			$this->config->get('devour.map.source'),
			$this->config->get('devour.map.target'),
			$this->config->get('devour.map.key')
		);

		foreach ($this->config->get('devour.map.fields', []) as $alias => $target) {
			$this->mapping->addField($alias, $target);
		}

		foreach ($this->config->get('devour.map.joins', []) as $target => $mapping) {
			foreach ($mapping as $alias => $conditions) {
				$this->mapping->addJoin($alias, $target, $conditions);
			}
		}

		foreach ($this->config->get('devour.map.require', []) as $requirement) {
			$this->mapping->addDependency($requirement);
		}

		foreach ($this->config->get('devour.map.updateWheres', []) as $condition) {
			$this->mapping->addUpdateWhere($condition);
		}

		foreach ($this->config->get('devour.map.wheres', []) as $condition) {
			$this->mapping->addWhere($condition);
		}

		foreach ($this->config->get('devour.map.filters', []) as $field => $filters) {
			if (!is_array($filters)) {
				$filters = array_map('trim', explode(',', $filters));
			}

			foreach ($filters as $filter) {
				$this->mapping->addFilter($field, $filter);
			}
		}

		foreach ($this->config->get('devour.map.params', []) as $param => $value) {
			$this->mapping->addParam($param, $value);
		}
	}


	public function testSourceExistingKeysQuery()
	{
		$this->assertEquals(
			$this->mapping->composeSourceExistingKeysQuery(),
			"SELECT events.control as id FROM evmas evmas LEFT JOIN firms vendor ON events.vendr = vendor.firm LEFT JOIN firms location ON events.fac = location.firm LEFT JOIN names facilitator ON events.admin = facilitator.id WHERE NULL IS NULL"
		);
	}

	public function testSourceUpdatedKeysQuery()
	{
		$this->mapping->addParam('last_synced', '2019-01-01');

		$this->assertEquals(
			$this->mapping->composeSourceUpdatedKeysQuery([['id' => 1], ['id' => 2], ['id' => 3]]),
			"SELECT events.control as id FROM evmas evmas LEFT JOIN firms vendor ON events.vendr = vendor.firm LEFT JOIN firms location ON events.fac = location.firm LEFT JOIN names facilitator ON events.admin = facilitator.id WHERE (events.adate >= '2019-01-01' OR events.udate >= '2019-01-01') AND (RTRIM(LTRIM(events.control)) = 1 OR RTRIM(LTRIM(events.control)) = 2 OR RTRIM(LTRIM(events.control)) = 3)"
		);

		$this->assertEquals(
			$this->mapping->composeSourceUpdatedKeysQuery([['id' => '1'], ['id' => '2'], ['id' => '3']]),
			"SELECT events.control as id FROM evmas evmas LEFT JOIN firms vendor ON events.vendr = vendor.firm LEFT JOIN firms location ON events.fac = location.firm LEFT JOIN names facilitator ON events.admin = facilitator.id WHERE (events.adate >= '2019-01-01' OR events.udate >= '2019-01-01') AND (RTRIM(LTRIM(events.control)) = '1' OR RTRIM(LTRIM(events.control)) = '2' OR RTRIM(LTRIM(events.control)) = '3')"
		);
	}

	public function testSourceDeleteSelectQuery()
	{
		$this->assertEquals(
			$this->mapping->composeSourceDeleteSelectQuery([['id' => 1], ['id' => 2], ['id' => 3]]),
			"SELECT events.* from devour_temp_events RIGHT OUTER JOIN events ON (devour_temp_events.id = events.id) WHERE devour_temp_events.id IS NULL AND (events.id = 1 OR events.id = 2 OR events.id = 3)"
		);

		$this->assertEquals(
			$this->mapping->composeSourceDeleteSelectQuery([['id' => '1'], ['id' => '2'], ['id' => '3']]),
			"SELECT events.* from devour_temp_events RIGHT OUTER JOIN events ON (devour_temp_events.id = events.id) WHERE devour_temp_events.id IS NULL AND (events.id = '1' OR events.id = '2' OR events.id = '3')"
		);
	}

	public function testDestinationExistingKeysQuery()
	{
		$this->assertEquals(
			$this->mapping->composeDestinationExistingKeysQuery(),
			"SELECT id FROM events"
		);
	}

	public function testSourceSelectQuery()
	{
		$this->assertEquals(
			$this->mapping->composeSourceSelectQuery([['id' => 1], ['id' => 2], ['id' => 3]]),
			"SELECT events.control as id, events.code1 as code, (RTRIM(LTRIM(events.emtitle1)) + ' ' + RTRIM(LTRIM(events.emtitle2))) as title, vendor.firm as vendor, location.firm as location, facilitator.id as facilitator, events.markdesc as description, events.ss as status, events.begdate as start_date, events.begtime as start_time, events.enddate as end_date, events.endtime as end_time, events.timezone as timezone FROM evmas evmas LEFT JOIN firms vendor ON events.vendr = vendor.firm LEFT JOIN firms location ON events.fac = location.firm LEFT JOIN names facilitator ON events.admin = facilitator.id WHERE NULL IS NULL AND (RTRIM(LTRIM(events.control)) = 1 OR RTRIM(LTRIM(events.control)) = 2 OR RTRIM(LTRIM(events.control)) = 3)"
		);
	}
}
