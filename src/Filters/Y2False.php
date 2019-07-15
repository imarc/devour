<?php

namespace Devour;

/**
 *
 */
class Y2False
{
	/**
	 *
	 */
	public function __invoke($value, array $context = array())
	{
		return strtolower($value) != 'y';
	}
}
