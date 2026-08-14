<?php

namespace Devour\Migrations;

use PDO;

interface Migration
{
	public function getId(): int;

	public function getDescription(): string;

	public function up(PDO $database): void;
}
