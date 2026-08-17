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


	/**
	 * schedule() stores json_encode($mappings), so a full sync arrives as '[]' rather than NULL.
	 */
	public function testEmptyJsonListsCountAsUnspecified()
	{
		$this->assertNull((new Devour\Run($this->row(['tables' => '[]'])))->getContext());
		$this->assertNull((new Devour\Run($this->row(['tables' => '[]', 'ids' => '[]'])))->getContext());

		$this->assertSame('limited', (new Devour\Run($this->row([
			'tables' => '["people"]',
			'ids'    => '[]',
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


	private function running(int $silenceSeconds, ?int $baseline): Devour\Run
	{
		$now  = strtotime('2026-08-17 12:00:00');
		$beat = date('Y-m-d H:i:s', $now - $silenceSeconds);

		return (new Devour\Run($this->row(['heartbeat' => $beat])))->withBaseline($baseline);
	}


	public function testConfidenceIsUnknownWithoutABaseline()
	{
		$run = $this->running(99999, NULL);

		$this->assertSame(Devour\Run::UNKNOWN, $run->getConfidence(strtotime('2026-08-17 12:00:00')));
	}


	public function testConfidenceIsUnknownWhenBaselineIsZero()
	{
		$run = $this->running(99999, 0);

		$this->assertSame(Devour\Run::UNKNOWN, $run->getConfidence(strtotime('2026-08-17 12:00:00')));
	}


	public function testSilenceFloorKeepsBriefPausesHealthy()
	{
		// 60s of silence against a 2s baseline is a ratio of 30, but under the floor.
		$run = $this->running(60, 2);

		$this->assertSame(Devour\Run::HEALTHY, $run->getConfidence(strtotime('2026-08-17 12:00:00')));
	}


	public function testConfidenceBands()
	{
		$now = strtotime('2026-08-17 12:00:00');

		// baseline 600s: healthy at or under 1x, suspect to 3x, stuck beyond.
		$this->assertSame(Devour\Run::HEALTHY, $this->running(600,  600)->getConfidence($now));
		$this->assertSame(Devour\Run::SUSPECT, $this->running(1200, 600)->getConfidence($now));
		$this->assertSame(Devour\Run::SUSPECT, $this->running(1800, 600)->getConfidence($now));
		$this->assertSame(Devour\Run::STUCK,   $this->running(1801, 600)->getConfidence($now));
	}


	public function testConfidenceIsUnknownForRunsThatAreNotRunning()
	{
		$run = (new Devour\Run($this->row([
			'end_time'  => '2026-08-17 09:30:00',
			'heartbeat' => '2026-08-17 09:00:00',
		])))->withBaseline(60);

		$this->assertSame(Devour\Run::UNKNOWN, $run->getConfidence(strtotime('2026-08-17 12:00:00')));
	}


	public function testStallRatioReportsTheUnderlyingNumber()
	{
		$run = $this->running(1200, 600);

		$this->assertSame(2.0, $run->getStallRatio(strtotime('2026-08-17 12:00:00')));
	}
}
