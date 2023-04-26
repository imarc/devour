<?php

namespace Devour;

/**
 *
 */
class SkipEmpty
{
	/**
	 *
	 */
	public function __invoke($value, array $context = array())
	{
		if (empty($value)) {
			throw new SkipException;
		}

		return $value;
	}
}
