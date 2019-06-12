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
		return strtolower($value);
	}
}
