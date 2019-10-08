<?php

namespace Devour;

/**
 *
 */
class Trim
{
	/**
	 *
	 */
	public function __invoke($value, array $context = array())
	{
		if (is_string($value)) {
			$value = trim($value);
		}

		return $value !== '' ? $value : NULL;
	}
}
