<?php

namespace Devour;

/**
 *
 */
class Lower
{
	/**
	 *
	 */
	public function __invoke($value, array $context = array())
	{
		if (!is_string($value)) {
			return $value;
		}

		return strtolower($value);
	}
}
