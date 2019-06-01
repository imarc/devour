<?php

use PHPUnit\Framework\TestCase;

final class MappingTest extends TestCase
{
	public function setUp(): void
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
			$this->mapping->addRequirement($requirement);
		}

		foreach ($this->config->get('devour.map.updateWheres', []) as $condition) {
			$this->mapping->addWhere($condition, TRUE);
		}

		foreach ($this->config->get('devour.map.wheres', []) as $condition) {
			$this->mapping->addWhere($condition, FALSE);
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
			"SELECT events.control as id FROM evmas events LEFT JOIN firms location ON events.fac = location.firm LEFT JOIN names facilitator ON events.admin = facilitator.id WHERE TRUE"
		);
	}

	public function testSourceUpdatedKeysQuery()
	{
		$this->mapping->addParam('last_synced', '2019-01-01');

		$this->assertEquals(
			$this->mapping->composeSourceUpdatedKeysQuery([1, 2, 3]),
			"SELECT events.control as id FROM evmas events LEFT JOIN firms location ON events.fac = location.firm LEFT JOIN names facilitator ON events.admin = facilitator.id WHERE events.adate >= '2019-01-01' AND events.udate >= '2019-01-01' AND id IN(1, 2, 3)"
		);

		$this->assertEquals(
			$this->mapping->composeSourceUpdatedKeysQuery(['1', '2', '3']),
			"SELECT events.control as id FROM evmas events LEFT JOIN firms location ON events.fac = location.firm LEFT JOIN names facilitator ON events.admin = facilitator.id WHERE events.adate >= '2019-01-01' AND events.udate >= '2019-01-01' AND id IN('1', '2', '3')"
		);
	}

	public function testDestinationDeleteQuery()
	{
		$this->assertEquals(
			$this->mapping->composeDestinationDeleteQuery([1, 2, 3]),
			"DELETE FROM events WHERE id IN(1, 2, 3)"
		);

		$this->assertEquals(
			$this->mapping->composeDestinationDeleteQuery(['1', '2', '3']),
			"DELETE FROM events WHERE id IN('1', '2', '3')"
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
			$this->mapping->composeSourceSelectQuery([1, 2, 3]),
			"SELECT events.control as id, events.code1 as code, (RTRIM(LTRIM(events.emtitle1)) + ' ' + RTRIM(LTRIM(events.emtitle2))) as title, vendor.firm as vendor, location.firm as location, facilitator.id as facilitator, events.markdesc as description, events.ss as status, events.begdate as start_date, events.begtime as start_time, events.enddate as end_date, events.endtime as end_time, events.timezone as timezone FROM evmas events LEFT JOIN firms location ON events.fac = location.firm LEFT JOIN names facilitator ON events.admin = facilitator.id WHERE id IN(1, 2, 3)"
		);
	}
}
