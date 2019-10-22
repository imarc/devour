<?php

namespace Devour;

/**
 *
 */
class Str2Int
{
	/**
	 *
	 */
	public function __invoke($value, array $context = array())
	{
		if (is_int($value)) {
			return $value;
		}

		if (is_string($value)) {
			$value = trim($value);
		}

		if (!is_numeric($value)) {
			return base_convert($value, 36, 10);
		}

		return $value;
	}
}
