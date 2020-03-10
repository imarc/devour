<?php

namespace Devour;

/**
 *
 */
class N2True
{
	/**
	 *
	 */
	public function __invoke($value, array $context = array())
	{
		return strtolower($value) == 'n';
	}
}
