<?php
declare(strict_types = 1);

namespace Simbiat\FFXIV;

class CrossworldLinkshell extends Linkshell
{
    #Custom properties
    protected const bool CROSSWORLD = true;
    protected string $idFormat = '/^[a-z0-9]{40}$/m';
}
