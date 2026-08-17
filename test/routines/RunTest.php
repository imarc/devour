<?php

use PHPUnit\Framework\TestCase;

final class RunTest extends TestCase
{
	private function row(array $overrides = []): array
	{
		return $overrides + [
			'id'             => 1,
			'start_time'     => '2026-08-17 09:00:00',
			'scheduled_by'   => 'admin@example.com',
			'scheduled_time' => NULL,
			'end_time'       => NULL,
			'canceled_time'  => NULL,
			'canceled_by'    => NULL,
			'heartbeat'      => NULL,
			'max_gap'        => NULL,
			'tables'         => NULL,
			'ids'            => NULL,
			'force'          => 0,
			'log'            => NULL,
		];
	}


	public function testRunningRequiresStartedUnfinishedAndUncancelled()
	{
		$this->assertTrue((new Devour\Run($this->row()))->isRunning());

		$this->assertFalse((new Devour\Run($this->row(['start_time' => NULL])))->isRunning());
		$this->assertFalse((new Devour\Run($this->row(['end_time' => '2026-08-17 09:30:00'])))->isRunning());
		$this->assertFalse((new Devour\Run($this->row(['canceled_time' => '2026-08-17 09:30:00'])))->isRunning());
	}


	public function testStatePredicatesAreMutuallyConsistent()
	{
		$scheduled = new Devour\Run($this->row([
			'start_time'     => NULL,
			'scheduled_time' => '2026-08-17 08:00:00',
		]));
		$complete  = new Devour\Run($this->row(['end_time' => '2026-08-17 09:30:00']));
		$cancelled = new Devour\Run($this->row(['canceled_time' => '2026-08-17 09:30:00']));

		$this->assertTrue($scheduled->isScheduled());
		$this->assertFalse($scheduled->isRunning());

		$this->assertTrue($complete->isComplete());
		$this->assertFalse($complete->isRunning());

		$this->assertTrue($cancelled->isCanceled());
		$this->assertFalse($cancelled->isComplete());
	}


	public function testContextMirrorsTablesAndIds()
	{
		$this->assertNull((new Devour\Run($this->row()))->getContext());

		$this->assertSame('limited', (new Devour\Run($this->row([
			'tables' => '["people"]',
		])))->getContext());

		$this->assertSame('individual', (new Devour\Run($this->row([
			'tables' => '["people"]',
			'ids'    => '[{"id":7}]',
		])))->getContext());
	}


	public function testSilenceMeasuresFromHeartbeatWhenPresent()
	{
		$run = new Devour\Run($this->row(['heartbeat' => '2026-08-17 09:50:00']));

		$this->assertSame(600, $run->getSilence(strtotime('2026-08-17 10:00:00')));
	}


	public function testSilenceFallsBackToStartTimeForRowsWithoutHeartbeat()
	{
		$run = new Devour\Run($this->row());

		$this->assertSame(3600, $run->getSilence(strtotime('2026-08-17 10:00:00')));
	}


	public function testSilenceIsNullBeforeAnythingStarts()
	{
		$run = new Devour\Run($this->row(['start_time' => NULL]));

		$this->assertNull($run->getSilence(strtotime('2026-08-17 10:00:00')));
	}
}
