<?php

namespace Devour;

/**
 * A single devour_stats row.
 *
 * Constructible from a raw row array so consumers holding an already-fetched row — an admin tool
 * rendering a list, for example — can ask about it without another query.  Everything here is pure;
 * the confidence assessment added on top needs a baseline supplied by Supervisor.
 */
class Run
{
	/**
	 * The running predicate, as SQL.
	 *
	 * This exists twice on purpose: here for filtering a SELECT, and in isRunning() for judging a
	 * row already in hand.  A SELECT cannot be filtered by a PHP method, so the duplication is
	 * unavoidable — it is kept adjacent, and SupervisorTest pins the two against the same fixtures.
	 */
	const WHERE_RUNNING = 'start_time IS NOT NULL AND end_time IS NULL AND canceled_time IS NULL';


	/**
	 * Context predicates, as SQL, keyed the way getContext() names them.
	 *
	 * An empty JSON list counts as "not specified".  schedule() stores json_encode($mappings), so a
	 * full sync scheduled through a UI arrives as the string '[]' rather than NULL, and testing only
	 * for NULL filed those runs under 'limited' — mixing hours-long full syncs into the bucket a
	 * single-table sync is estimated from.
	 */
	const WHERE_CONTEXT = [
		'individual' => "(tables IS NOT NULL AND tables <> '[]') AND (ids IS NOT NULL AND ids <> '[]')",
		'limited'    => "(tables IS NOT NULL AND tables <> '[]') AND (ids IS NULL OR ids = '[]')",
		''           => "(tables IS NULL OR tables = '[]')",
	];


	const HEALTHY = 'healthy';
	const SUSPECT = 'suspect';
	const STUCK   = 'stuck';
	const UNKNOWN = 'unknown';


	/**
	 * Nothing rates worse than healthy before this much silence, whatever the baseline says.
	 *
	 * Without it a site whose largest recorded gap is two seconds would rate a seven-second pause
	 * as stuck.
	 */
	const SILENCE_FLOOR = 300;


	const SUSPECT_RATIO = 1.0;
	const STUCK_RATIO   = 3.0;


	/**
	 *
	 */
	protected array $row = [];


	/**
	 *
	 */
	protected ?int $baseline = NULL;


	/**
	 *
	 */
	public function __construct(array $row)
	{
		$this->row = $row;
	}


	/**
	 *
	 */
	public function toArray(): array
	{
		return $this->row;
	}


	/**
	 *
	 */
	public function getId(): ?int
	{
		return isset($this->row['id']) ? (int) $this->row['id'] : NULL;
	}


	/**
	 *
	 */
	public function getStartTime(): ?string
	{
		return $this->value('start_time');
	}


	/**
	 *
	 */
	public function getEndTime(): ?string
	{
		return $this->value('end_time');
	}


	/**
	 *
	 */
	public function getCanceledTime(): ?string
	{
		return $this->value('canceled_time');
	}


	/**
	 *
	 */
	public function getCanceledBy(): ?string
	{
		return $this->value('canceled_by');
	}


	/**
	 *
	 */
	public function getHeartbeat(): ?string
	{
		return $this->value('heartbeat');
	}


	/**
	 *
	 */
	public function getMaxGap(): ?int
	{
		$gap = $this->value('max_gap');

		return $gap === NULL ? NULL : (int) $gap;
	}


	/**
	 *
	 */
	public function getLog(): ?string
	{
		return $this->row['log'] ?? NULL;
	}


	/**
	 * The run's failures, aggregated by table and message.
	 *
	 * Held apart from the log so it can be shown or emailed on its own.  A full sync's log is tens
	 * of thousands of lines and its failures are a handful of distinct problems repeated; this is
	 * the handful.
	 */
	public function getError(): ?string
	{
		return $this->row['error'] ?? NULL;
	}


	/**
	 *
	 */
	public function hasError(): bool
	{
		return trim((string) $this->getError()) !== '';
	}


	/**
	 * Per-table figures, keyed by destination table.
	 *
	 * @return array<string, array>
	 */
	public function getTableStats(): array
	{
		$decoded = json_decode((string) ($this->row['table_stats'] ?? ''), TRUE);

		return is_array($decoded) ? $decoded : [];
	}


	/**
	 *
	 */
	public function getScheduledBy(): ?string
	{
		return $this->value('scheduled_by');
	}


	/**
	 *
	 */
	public function getScheduledTime(): ?string
	{
		return $this->value('scheduled_time');
	}


	/**
	 *
	 */
	public function getTables(): ?string
	{
		return $this->value('tables');
	}


	/**
	 *
	 */
	public function getIds(): ?string
	{
		return $this->value('ids');
	}


	/**
	 * The tables this run was limited to, or an empty list meaning every mapping.
	 */
	public function getTableList(): array
	{
		if ($this->isEmptyList($this->getTables())) {
			return [];
		}

		$decoded = json_decode($this->getTables(), TRUE);

		return is_array($decoded) ? $decoded : [];
	}


	/**
	 * Was this run asked to update every row rather than only those changed since the watermark?
	 *
	 * The column is boolean in PostgreSQL but reaches PHP as a driver-dependent scalar, so the
	 * spellings a boolean can arrive as are all accepted.
	 */
	public function isForced(): bool
	{
		$force = $this->row['force'] ?? NULL;

		if (is_bool($force)) {
			return $force;
		}

		return in_array(strtolower((string) $force), ['1', 't', 'true', 'on'], TRUE);
	}


	/**
	 * Which historical bucket this run should be compared against.
	 *
	 * Mirrors Synchronizer::getSyncInterval()'s buckets exactly.  A full sync's log gaps are
	 * legitimately far larger than a single-record sync's, so they cannot share a baseline.
	 */
	public function getContext(): ?string
	{
		if ($this->isEmptyList($this->getTables())) {
			return NULL;
		}

		return $this->isEmptyList($this->getIds()) ? 'limited' : 'individual';
	}


	/**
	 * Is this column unset, or set to an empty JSON list?
	 *
	 * Compared as a literal string rather than decoded, so this stays identical to the SQL in
	 * WHERE_CONTEXT.  json_encode() emits exactly '[]' for an empty array, so there is no other
	 * spelling to account for.
	 */
	protected function isEmptyList(?string $json): bool
	{
		return $json === NULL || $json === '[]';
	}


	/**
	 *
	 */
	public function isRunning(): bool
	{
		return $this->getStartTime() !== NULL
			&& $this->getEndTime() === NULL
			&& $this->getCanceledTime() === NULL;
	}


	/**
	 *
	 */
	public function isCanceled(): bool
	{
		return $this->getCanceledTime() !== NULL;
	}


	/**
	 *
	 */
	public function isComplete(): bool
	{
		return $this->getEndTime() !== NULL && $this->getCanceledTime() === NULL;
	}


	/**
	 *
	 */
	public function isScheduled(): bool
	{
		return $this->getStartTime() === NULL
			&& $this->getScheduledTime() !== NULL
			&& $this->getCanceledTime() === NULL;
	}


	/**
	 * Seconds since this run last showed a sign of life.
	 *
	 * Falling back onto start_time is what makes rows written before Migration002 work without a
	 * special case: they have no heartbeat, so their silence is measured from when they began.
	 */
	public function getSilence(?int $now = NULL): ?int
	{
		$last = $this->getHeartbeat() ?: $this->getStartTime();

		if ($last === NULL) {
			return NULL;
		}

		//
		// Clamped at zero.  A heartbeat in the future means clock skew, or a PHP timezone that no
		// longer matches whatever wrote the row — neither of which is evidence that the run has
		// stalled, and a negative silence would otherwise be reported as a negative stall ratio.
		//
		return max(0, ($now ?: time()) - strtotime($last));
	}


	/**
	 *
	 */
	public function withBaseline(?int $baseline): self
	{
		$this->baseline = $baseline;

		return $this;
	}


	/**
	 *
	 */
	public function getBaseline(): ?int
	{
		return $this->baseline;
	}


	/**
	 *
	 */
	public function getStallRatio(?int $now = NULL): ?float
	{
		$silence = $this->getSilence($now);

		if ($silence === NULL || !$this->baseline) {
			return NULL;
		}

		return $silence / $this->baseline;
	}


	/**
	 * How confident we are that this run has stopped making progress.
	 *
	 * Every threshold biases toward HEALTHY.  The verdict never blocks a cancellation, so a false
	 * healthy costs an operator one extra glance, while a false stuck on a cron running with
	 * --cancel destroys work that was going fine.
	 */
	public function getConfidence(?int $now = NULL): string
	{
		if (!$this->isRunning()) {
			return self::UNKNOWN;
		}

		$silence = $this->getSilence($now);

		if ($silence === NULL || $this->baseline === NULL || $this->baseline <= 0) {
			return self::UNKNOWN;
		}

		if ($silence < self::SILENCE_FLOOR) {
			return self::HEALTHY;
		}

		$ratio = $silence / $this->baseline;

		if ($ratio <= self::SUSPECT_RATIO) {
			return self::HEALTHY;
		}

		if ($ratio <= self::STUCK_RATIO) {
			return self::SUSPECT;
		}

		return self::STUCK;
	}


	/**
	 *
	 */
	public function getSummary(?int $now = NULL): string
	{
		$confidence = $this->getConfidence($now);
		$silence    = $this->getSilence($now);

		if ($silence === NULL) {
			return sprintf('Sync %s has not started.', $this->getId());
		}

		if ($this->baseline === NULL || $this->baseline <= 0) {
			return sprintf(
				'Sync %s: %s, silent %ds, no comparable history yet.',
				$this->getId(),
				$confidence,
				$silence
			);
		}

		return sprintf(
			'Sync %s: %s, silent %ds against a %ds baseline (%.1fx).',
			$this->getId(),
			$confidence,
			$silence,
			$this->baseline,
			$silence / $this->baseline
		);
	}


	/**
	 *
	 */
	protected function value(string $column): ?string
	{
		$value = $this->row[$column] ?? NULL;

		return ($value === NULL || $value === '') ? NULL : (string) $value;
	}
}
