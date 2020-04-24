<?php

namespace Devour;

/**
 *
 */
class Y2True
{
	/**
	 *
	 */
	public function __invoke($value, array $context = array())
	{
		return strtolower($value[0] ?? NULL) == 'y';
	}
}
