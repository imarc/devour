<?php

namespace Devour;

use PDO;

/**
 *
 */
interface FileDriver
{
	/**
	 *
	 */
	public function supports(Mapping $mapping);


	/**
	 *
	 */
	public function getAlias(Mapping $mapping);


	/**
	 *
	 */
	public function materialize(PDO $database, Mapping $mapping);
}
