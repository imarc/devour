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
		return trim($value);
	}
}
