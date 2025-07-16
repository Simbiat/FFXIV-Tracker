<?php
declare(strict_types = 1);

namespace Simbiat\FFXIV;

class CrossworldLinkshell extends Linkshell
{
    #Custom properties
    protected const bool CROSSWORLD = true;
    protected string $id_format = '/^[a-z0-9]{40}$/m';
}
