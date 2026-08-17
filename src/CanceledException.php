<?php

namespace Devour;

use RuntimeException;

/**
 * Thrown when a running sync discovers it has been cancelled.
 *
 * Synchronizer detects this from the row count of the UPDATE it already issues on every log line,
 * so cancellation costs no extra query and is noticed within one log line of being requested.
 */
class CanceledException extends RuntimeException
{
}
