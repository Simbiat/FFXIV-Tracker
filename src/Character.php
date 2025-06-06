<?php
declare(strict_types = 1);

namespace Simbiat\FFXIV;

use Simbiat\Cron\TaskInstance;
use Simbiat\Database\Query;
use Simbiat\Website\Config;
use Simbiat\Website\Errors;
use Simbiat\Website\Images;
use Simbiat\Website\Sanitization;
use Simbiat\Website\Security;
use Simbiat\Website\usercontrol\User;
use function is_array;

/**
 * Class representing a FFXIV character
 */
class Character extends AbstractTrackerEntity
{
    #Custom properties
    public ?string $avatarID = '';
    public array $dates = [];
    public array $biology = [];
    public array $location = [];
    public ?string $biography = null;
    public array $title = [];
    public array $grandCompany = [];
    public int $pvp = 0;
    public array $groups = [];
    public array $jobs = [];
    public array $achievements = [];
    public int $achievementPoints = 0;
    public array $owned = [];
    
    /**
     * Function to get initial data from DB
     * @throws \Exception
     */
    protected function getFromDB(): array
    {
        #Get general information. Using *, but add name, because otherwise Achievement name overrides Character name, and we do not want that
        $data = Query::query('SELECT *, `ffxiv__character`.`characterid`, `ffxiv__achievement`.`icon` AS `titleIcon`, `ffxiv__character`.`name`, `ffxiv__character`.`registered`, `ffxiv__character`.`updated` FROM `ffxiv__character`LEFT JOIN `uc__user_to_ff_character` ON `uc__user_to_ff_character`.`characterid`=`ffxiv__character`.`characterid` LEFT JOIN `ffxiv__clan` ON `ffxiv__character`.`clanid` = `ffxiv__clan`.`clanid` LEFT JOIN `ffxiv__guardian` ON `ffxiv__character`.`guardianid` = `ffxiv__guardian`.`guardianid` LEFT JOIN `ffxiv__nameday` ON `ffxiv__character`.`namedayid` = `ffxiv__nameday`.`namedayid` LEFT JOIN `ffxiv__city` ON `ffxiv__character`.`cityid` = `ffxiv__city`.`cityid` LEFT JOIN `ffxiv__server` ON `ffxiv__character`.`serverid` = `ffxiv__server`.`serverid` LEFT JOIN `ffxiv__grandcompany_rank` ON `ffxiv__character`.`gcrankid` = `ffxiv__grandcompany_rank`.`gcrankid` LEFT JOIN `ffxiv__grandcompany` ON `ffxiv__grandcompany_rank`.`gcId` = `ffxiv__grandcompany`.`gcId` LEFT JOIN `ffxiv__achievement` ON `ffxiv__character`.`titleid` = `ffxiv__achievement`.`achievementid` WHERE `ffxiv__character`.`characterid` = :id;', [':id' => $this->id], return: 'row');
        if (!empty($data['privated'])) {
            foreach ($data as $key => $value) {
                if (!\in_array($key, ['avatar', 'registered', 'updated', 'deleted', 'privated', 'name'])) {
                    unset($data[$key]);
                }
            }
            return $data;
        }
        #Return empty if nothing was found
        if (empty($data)) {
            return [];
        }
        #Get username if character is linked to a user
        if (!empty($data['userid'])) {
            $data['username'] = Query::query('SELECT `username` FROM `uc__users` WHERE `userid`=:userid;', [':userid' => $data['userid']], return: 'value');
        } else {
            $data['username'] = null;
        }
        #Get jobs
        $data['jobs'] = Query::query('SELECT `name`, `level`, `last_change` FROM `ffxiv__character_jobs` LEFT JOIN `ffxiv__jobs` ON `ffxiv__character_jobs`.`jobid`=`ffxiv__jobs`.`jobid` WHERE `ffxiv__character_jobs`.`characterid`=:id ORDER BY `name`;', [':id' => $this->id], return: 'all');
        #Get old names. For now returning only the count due to cases of bullying, when the old names are learnt. They are still being collected, though, for statistical purposes.
        $data['oldNames'] = Query::query('SELECT `name` FROM `ffxiv__character_names` WHERE `characterid`=:id AND `name`!=:name', [':id' => $this->id, ':name' => $data['name']], return: 'column');
        #Get previous known incarnations (combination of gender and race/clan)
        $data['incarnations'] = Query::query('SELECT `genderid`, `ffxiv__clan`.`race`, `ffxiv__clan`.`clan` FROM `ffxiv__character_clans` LEFT JOIN `ffxiv__clan` ON `ffxiv__character_clans`.`clanid` = `ffxiv__clan`.`clanid` WHERE `ffxiv__character_clans`.`characterid`=:id AND (`ffxiv__character_clans`.`clanid`!=:clanid AND `ffxiv__character_clans`.`genderid`!=:genderid) ORDER BY `genderid` , `race` , `clan` ', [':id' => $this->id, ':clanid' => $data['clanid'], ':genderid' => $data['genderid']], return: 'all');
        #Get old servers
        $data['servers'] = Query::query('SELECT `ffxiv__server`.`datacenter`, `ffxiv__server`.`server` FROM `ffxiv__character_servers` LEFT JOIN `ffxiv__server` ON `ffxiv__server`.`serverid`=`ffxiv__character_servers`.`serverid` WHERE `ffxiv__character_servers`.`characterid`=:id AND `ffxiv__character_servers`.`serverid` != :serverid ORDER BY `datacenter` , `server` ', [':id' => $this->id, ':serverid' => $data['serverid']], return: 'all');
        #Get achievements
        $data['achievements'] = Query::query('SELECT \'achievement\' AS `type`, `ffxiv__achievement`.`achievementid` AS `id`, `ffxiv__achievement`.`category`, `ffxiv__achievement`.`subcategory`, `ffxiv__achievement`.`name`, `time`, `icon` FROM `ffxiv__character_achievement` LEFT JOIN `ffxiv__achievement` ON `ffxiv__character_achievement`.`achievementid`=`ffxiv__achievement`.`achievementid` WHERE `ffxiv__character_achievement`.`characterid` = :id AND `ffxiv__achievement`.`category` IS NOT NULL AND `ffxiv__achievement`.`achievementid` IS NOT NULL ORDER BY `time` DESC, `name` LIMIT 10', [':id' => $this->id], return: 'all');
        #Get affiliated groups' details
        $data['groups'] = AbstractTrackerEntity::cleanCrestResults(Query::query(
        /** @lang SQL */ '(SELECT \'freecompany\' AS `type`, 0 AS `crossworld`, `ffxiv__freecompany_character`.`freecompanyid` AS `id`, `ffxiv__freecompany`.`name` as `name`, `current`, `ffxiv__freecompany_character`.`rankid`, `ffxiv__freecompany_rank`.`rankname` AS `rank`, `crest_part_1`, `crest_part_2`, `crest_part_3`, `grandcompanyid` FROM `ffxiv__freecompany_character` LEFT JOIN `ffxiv__freecompany` ON `ffxiv__freecompany_character`.`freecompanyid`=`ffxiv__freecompany`.`freecompanyid` LEFT JOIN `ffxiv__freecompany_rank` ON `ffxiv__freecompany_rank`.`freecompanyid`=`ffxiv__freecompany`.`freecompanyid` AND `ffxiv__freecompany_character`.`rankid`=`ffxiv__freecompany_rank`.`rankid` WHERE `characterid`=:id)
            UNION ALL
            (SELECT \'linkshell\' AS `type`, `crossworld`, `ffxiv__linkshell_character`.`linkshellid` AS `id`, `ffxiv__linkshell`.`name` as `name`, `current`, `ffxiv__linkshell_character`.`rankid`, `ffxiv__linkshell_rank`.`rank` AS `rank`, null as `crest_part_1`, null as `crest_part_2`, null as `crest_part_3`, null as `grandcompanyid` FROM `ffxiv__linkshell_character` LEFT JOIN `ffxiv__linkshell` ON `ffxiv__linkshell_character`.`linkshellid`=`ffxiv__linkshell`.`linkshellid` LEFT JOIN `ffxiv__linkshell_rank` ON `ffxiv__linkshell_character`.`rankid`=`ffxiv__linkshell_rank`.`lsrankid` WHERE `characterid`=:id)
            UNION ALL
            (SELECT \'pvpteam\' AS `type`, 1 AS `crossworld`, `ffxiv__pvpteam_character`.`pvpteamid` AS `id`, `ffxiv__pvpteam`.`name` as `name`, `current`, `ffxiv__pvpteam_character`.`rankid`, `ffxiv__pvpteam_rank`.`rank` AS `rank`, `crest_part_1`, `crest_part_2`, `crest_part_3`, null as `grandcompanyid` FROM `ffxiv__pvpteam_character` LEFT JOIN `ffxiv__pvpteam` ON `ffxiv__pvpteam_character`.`pvpteamid`=`ffxiv__pvpteam`.`pvpteamid` LEFT JOIN `ffxiv__pvpteam_rank` ON `ffxiv__pvpteam_character`.`rankid`=`ffxiv__pvpteam_rank`.`pvprankid` WHERE `characterid`=:id)
            ORDER BY `current` DESC, `name`;',
            [':id' => $this->id], return: 'all'
        ));
        #Clean up the data from unnecessary (technical) clutter
        unset($data['clanid'], $data['namedayid'], $data['achievementid'], $data['category'], $data['subcategory'], $data['howto'], $data['points'], $data['icon'], $data['item'], $data['itemicon'], $data['itemid'], $data['serverid']);
        #In case the entry is old enough (at least 1 day old) and register it for update. Also check that this is not a bot.
        if (empty($_SESSION['UA']['bot']) && (time() - strtotime($data['updated'])) >= 86400) {
            new TaskInstance()->settingsFromArray(['task' => 'ffUpdateEntity', 'arguments' => [(string)$this->id, 'character'], 'message' => 'Updating character with ID '.$this->id, 'priority' => 1])->add();
        }
        return $data;
    }
    
    /**
     * Get character data from Lodestone
     * @param bool $allowSleep Whether to wait in case Lodestone throttles the request (that is throttle on our side)
     *
     * @return string|array
     */
    public function getFromLodestone(bool $allowSleep = false): string|array
    {
        $Lodestone = (new Lodestone());
        $data = $Lodestone->getCharacter($this->id)->getCharacterJobs($this->id)->getResult();
        #Check if the character is private
        if (isset($data['characters'][$this->id]['private']) && $data['characters'][$this->id]['private'] === true) {
            $this->markPrivate();
            return $data['characters'][$this->id];
        }
        #Check for possible errors
        if (empty($data['characters'][$this->id]['server'])) {
            if (!empty($data['characters'][$this->id]) && (int)$data['characters'][$this->id] === 404) {
                $this->delete();
                return ['404' => true];
            }
            #Take a pause if we were throttled, and pause is allowed
            if (!empty($Lodestone->getLastError()['error']) && preg_match('/Lodestone has throttled the request, 429/', $Lodestone->getLastError()['error']) === 1) {
                if ($allowSleep) {
                    sleep(60);
                }
                return 'Request throttled by Lodestone';
            }
            if (empty($Lodestone->getLastError())) {
                return 'Failed to get any data for Character '.$this->id;
            }
            return 'Failed to get all necessary data for Character '.$this->id.' ('.$Lodestone->getLastError()['url'].'): '.$Lodestone->getLastError()['error'];
        }
        #Try to get achievements now, that we got basic information, and there were no issues with it.
        $data = $Lodestone->getCharacterAchievements($this->id, false, 0, false, false, true)->getResult();
        $data = $data['characters'][$this->id];
        $data['id'] = $this->id;
        $data['404'] = false;
        return $data;
    }
    
    /**
     * Function to process data from DB
     * @param array $fromDB
     *
     * @return void
     */
    protected function process(array $fromDB): void
    {
        $this->name = $fromDB['name'];
        $this->avatarID = $fromDB['avatar'];
        $this->dates = [
            'registered' => strtotime($fromDB['registered']),
            'updated' => strtotime($fromDB['updated']),
            'privated' => (empty($fromDB['privated']) ? null : strtotime($fromDB['privated'])),
            'deleted' => (empty($fromDB['deleted']) ? null : strtotime($fromDB['deleted'])),
        ];
        $this->biology = [
            'gender' => (int)($fromDB['genderid'] ?? 0),
            'race' => $fromDB['race'] ?? null,
            'clan' => $fromDB['clan'] ?? null,
            'nameday' => $fromDB['nameday'] ?? null,
            'guardian' => $fromDB['guardian'] ?? null,
            'guardianid' => $fromDB['guardianid'] ?? null,
            'incarnations' => $fromDB['incarnations'] ?? null,
            'oldNames' => $fromDB['oldNames'] ?? [],
        ];
        $this->location = [
            'datacenter' => $fromDB['datacenter'] ?? null,
            'server' => $fromDB['server'] ?? null,
            'region' => $fromDB['region'] ?? null,
            'city' => $fromDB['city'] ?? null,
            'cityid' => $fromDB['cityid'] ?? null,
            'previousServers' => $fromDB['servers'] ?? [],
        ];
        $this->biography = $fromDB['biography'] ?? null;
        $this->title = [
            'title' => $fromDB['title'] ?? null,
            'icon' => $fromDB['titleIcon'] ?? null,
            'id' => $fromDB['titleid'] ?? null,
        ];
        $this->grandCompany = [
            'name' => $fromDB['gcName'] ?? null,
            'rank' => $fromDB['gc_rank'] ?? null,
            'gcId' => $fromDB['gcId'] ?? null,
            'gcrankid' => $fromDB['gcrankid'] ?? null,
        ];
        $this->pvp = (int)($fromDB['pvp_matches'] ?? 0);
        $this->groups = $fromDB['groups'] ?? [];
        $this->owned = [
            'id' => $fromDB['userid'] ?? null,
            'name' => $fromDB['username'] ?? null
        ];
        foreach ($this->groups as $key => $group) {
            $this->groups[$key]['current'] = (bool)$group['current'];
        }
        $this->achievements = $fromDB['achievements'] ?? [];
        foreach ($this->achievements as $key => $achievement) {
            $this->achievements[$key]['time'] = (empty($achievement['time']) ? null : strtotime($achievement['time']));
        }
        $this->achievementPoints = $fromDB['achievement_points'] ?? 0;
        $this->jobs = $fromDB['jobs'] ?? [];
    }
    
    /**
     * Function to update the entity
     *
     * @return bool
     */
    protected function updateDB(): bool
    {
        try {
            #Get time of the last update for the character if it exists on the tracker
            $updated = Query::query('SELECT `updated` FROM `ffxiv__character` WHERE `characterid`=:characterid', [':characterid' => $this->id], return: 'value');
            #If a character on Lodestone is not registered in Free Company or PvP Team, add their IDs as NULL for consistency
            if (empty($this->lodestone['freeCompany']['id'])) {
                $this->lodestone['freeCompany']['id'] = NULL;
                $this->lodestone['freeCompany']['registered'] = false;
            } else {
                $this->lodestone['freeCompany']['registered'] = Query::query('SELECT `freecompanyid` FROM `ffxiv__freecompany` WHERE `freecompanyid` = :id', [':id' => $this->lodestone['freeCompany']['id']], return: 'check');
            }
            if (empty($this->lodestone['pvp']['id'])) {
                $this->lodestone['pvp']['id'] = NULL;
                $this->lodestone['pvp']['registered'] = false;
            } else {
                $this->lodestone['pvp']['registered'] = Query::query('SELECT `pvpteamid` FROM `ffxiv__pvpteam` WHERE `pvpteamid` = :id', [':id' => $this->lodestone['pvp']['id']], return: 'check');
            }
            #Insert Free Companies and PvP Team if they are not registered
            if ($this->lodestone['freeCompany']['id'] !== NULL && $this->lodestone['freeCompany']['registered'] === false) {
                $queries[] = [
                    'INSERT IGNORE INTO `ffxiv__freecompany` (`freecompanyid`, `name`, `serverid`, `updated`) VALUES (:fcId, :fcName, (SELECT `serverid` FROM `ffxiv__server` WHERE `server`=:server), TIMESTAMPADD(SECOND, -3600, CURRENT_TIMESTAMP()));',
                    [
                        ':fcId' => $this->lodestone['freeCompany']['id'],
                        ':fcName' => $this->lodestone['freeCompany']['name'],
                        ':server' => $this->lodestone['server'],
                    ],
                ];
            }
            if ($this->lodestone['pvp']['id'] !== NULL && $this->lodestone['pvp']['registered'] === false) {
                $queries[] = [
                    'INSERT IGNORE INTO `ffxiv__pvpteam` (`pvpteamid`, `name`, `datacenterid`, `updated`) VALUES (:pvpId, :pvpName, (SELECT `serverid` FROM `ffxiv__server` WHERE `server`=:server), TIMESTAMPADD(SECOND, -3600, CURRENT_TIMESTAMP()));',
                    [
                        ':pvpId' => $this->lodestone['pvp']['id'],
                        ':pvpName' => $this->lodestone['pvp']['name'],
                        ':server' => $this->lodestone['server'],
                    ],
                ];
            }
            #Reduce the number of <br>s in biography
            $this->lodestone['bio'] = Sanitization::sanitizeHTML($this->lodestone['bio'] ?? '');
            if (($this->lodestone['bio'] === '-')) {
                $this->lodestone['bio'] = null;
            }
            #Get total achievements points. Using foreach for speed
            $achievementPoints = 0;
            if (!empty($this->lodestone['achievements']) && is_array($this->lodestone['achievements'])) {
                foreach ($this->lodestone['achievements'] as $item) {
                    $achievementPoints += (int)$item['points'];
                }
            }
            #Main query to insert or update a character
            $queries[] = [
                'INSERT INTO `ffxiv__character`(
                    `characterid`, `serverid`, `name`, `registered`, `updated`, `privated`, `deleted`, `biography`, `titleid`, `avatar`, `clanid`, `genderid`, `namedayid`, `guardianid`, `cityid`, `gcrankid`, `pvp_matches`, `achievement_points`
                )
                VALUES (
                    :characterid, (SELECT `serverid` FROM `ffxiv__server` WHERE `server`=:server), :name, UTC_DATE(), CURRENT_TIMESTAMP(), NULL, NULL, :biography, (SELECT `achievementid` as `titleid` FROM `ffxiv__achievement` WHERE `title` IS NOT NULL AND `title`=:title LIMIT 1), :avatar, (SELECT `clanid` FROM `ffxiv__clan` WHERE `clan`=:clan), :genderid, (SELECT `namedayid` FROM `ffxiv__nameday` WHERE `nameday`=:nameday), (SELECT `guardianid` FROM `ffxiv__guardian` WHERE `guardian`=:guardian), (SELECT `cityid` FROM `ffxiv__city` WHERE `city`=:city), `gcrankid` = (SELECT `gcrankid` FROM `ffxiv__grandcompany_rank` WHERE `gc_rank`=:gcRank ORDER BY `gcrankid` LIMIT 1), 0, :achievementPoints
                )
                ON DUPLICATE KEY UPDATE
                    `serverid`=(SELECT `serverid` FROM `ffxiv__server` WHERE `server`=:server), `name`=:name, `updated`=CURRENT_TIMESTAMP(), `privated`=NULL, `deleted`=NULL, `biography`=:biography, `titleid`=(SELECT `achievementid` as `titleid` FROM `ffxiv__achievement` WHERE `title` IS NOT NULL AND `title`=:title LIMIT 1), `avatar`=:avatar, `clanid`=(SELECT `clanid` FROM `ffxiv__clan` WHERE `clan`=:clan), `genderid`=:genderid, `namedayid`=(SELECT `namedayid` FROM `ffxiv__nameday` WHERE `nameday`=:nameday), `guardianid`=(SELECT `guardianid` FROM `ffxiv__guardian` WHERE `guardian`=:guardian), `cityid`=(SELECT `cityid` FROM `ffxiv__city` WHERE `city`=:city), `gcrankid`=(SELECT `gcrankid` FROM `ffxiv__grandcompany_rank` WHERE `gc_rank` IS NOT NULL AND `gc_rank`=:gcRank ORDER BY `gcrankid` LIMIT 1), `achievement_points`=:achievementPoints;',
                [
                    ':characterid' => $this->id,
                    ':server' => $this->lodestone['server'],
                    ':name' => $this->lodestone['name'],
                    ':avatar' => str_replace(['https://img2.finalfantasyxiv.com/f/', 'c0_96x96.jpg', 'c0.jpg'], '', $this->lodestone['avatar']),
                    ':biography' => [
                        (empty($this->lodestone['bio']) ? NULL : $this->lodestone['bio']),
                        (empty($this->lodestone['bio']) ? 'null' : 'string'),
                    ],
                    ':title' => (empty($this->lodestone['title']) ? '' : $this->lodestone['title']),
                    ':clan' => $this->lodestone['clan'],
                    ':genderid' => ($this->lodestone['gender'] === 'male' ? '1' : '0'),
                    ':nameday' => $this->lodestone['nameday'],
                    ':guardian' => $this->lodestone['guardian']['name'],
                    ':city' => $this->lodestone['city']['name'],
                    ':gcRank' => (empty($this->lodestone['grandCompany']['rank']) ? '' : $this->lodestone['grandCompany']['rank']),
                    ':achievementPoints' => [$achievementPoints, 'int']
                ],
            ];
            #Update levels. Doing this in a cycle since columns can vary. This can reduce performance, but so far this is the best idea I have to make it as automated as possible
            if (!empty($this->lodestone['jobs'])) {
                foreach ($this->lodestone['jobs'] as $job => $level) {
                    #Insert job, in case it's missing
                    $queries[] = ['INSERT IGNORE INTO `ffxiv__jobs` (`name`) VALUES (:job);', [':job' => $job]];
                    #Update level, but only if it's more than 0
                    if ((int)$level['level'] > 0) {
                        $queries[] = [
                            'INSERT INTO `ffxiv__character_jobs` (`characterid`, `jobid`, `level`, `last_change`) VALUES (:characterid, (SELECT `jobid` FROM `ffxiv__jobs` WHERE `name`=:jobname), :level, CURRENT_TIMESTAMP()) ON DUPLICATE KEY UPDATE `level`=:level, `last_change`=IF(`level`=:level, `last_change`, CURRENT_TIMESTAMP());',
                            [
                                ':characterid' => $this->id,
                                ':jobname' => $job,
                                ':level' => [(int)$level['level'], 'int'],
                            ],
                        ];
                    }
                }
            }
            $this->insertServerAndName($queries);
            #Insert race, clan and sex combination if it has not been inserted yet
            if (!empty($this->lodestone['clan'])) {
                $queries[] = [
                    'INSERT IGNORE INTO `ffxiv__character_clans`(`characterid`, `genderid`, `clanid`) VALUES (:characterid, :genderid, (SELECT `clanid` FROM `ffxiv__clan` WHERE `clan`=:clan));',
                    [
                        ':characterid' => $this->id,
                        ':genderid' => ($this->lodestone['gender'] === 'male' ? '1' : '0'),
                        ':clan' => $this->lodestone['clan'],
                    ],
                ];
            }
            #Update company information
            $queries[] = [
                'UPDATE `ffxiv__freecompany_character` SET `current`=0 WHERE `characterid`=:characterid AND `freecompanyid` '.(empty($this->lodestone['freeCompany']['id']) ? 'IS NOT ' : '!= ').' :fcId;',
                [
                    ':characterid' => $this->id,
                    ':fcId' => [
                        $this->lodestone['freeCompany']['id'],
                        (empty($this->lodestone['freeCompany']['id']) ? 'null' : 'string'),
                    ],
                ],
            ];
            #Update PvP Team information
            $queries[] = [
                'UPDATE `ffxiv__pvpteam_character` SET `current`=0 WHERE `characterid`=:characterid AND `pvpteamid` '.(empty($this->lodestone['pvp']['id']) ? 'IS NOT ' : '!= ').' :pvpId;',
                [
                    ':characterid' => $this->id,
                    ':pvpId' => [
                        $this->lodestone['pvp']['id'],
                        (empty($this->lodestone['pvp']['id']) ? 'null' : 'string'),
                    ],
                ],
            ];
            #Achievements
            if (!empty($this->lodestone['achievements']) && is_array($this->lodestone['achievements'])) {
                foreach ($this->lodestone['achievements'] as $achievementid => $item) {
                    $icon = self::removeLodestoneDomain($item['icon']);
                    #Download the icon if it's not already present
                    if (is_file(str_replace('.png', '.webp', Config::$icons.$icon))) {
                        $webp = true;
                    } else {
                        $webp = Images::download($item['icon'], Config::$icons.$icon);
                    }
                    if ($webp) {
                        $icon = str_replace('.png', '.webp', $icon);
                        $queries[] = [
                            'INSERT INTO `ffxiv__achievement` SET `achievementid`=:achievementid, `name`=:name, `icon`=:icon, `points`=:points ON DUPLICATE KEY UPDATE `updated`=`updated`, `name`=:name, `icon`=:icon, `points`=:points;',
                            [
                                ':achievementid' => $achievementid,
                                ':name' => $item['name'],
                                ':icon' => $icon,
                                ':points' => $item['points'],
                            ],
                        ];
                        #If the achievement is new since the last check, or if this is the first time the character is being processed, add and count the achievement
                        if (!empty($updated) && (int)$item['time'] > strtotime($updated)) {
                            $queries[] = [
                                'INSERT INTO `ffxiv__character_achievement` SET `characterid`=:characterid, `achievementid`=:achievementid, `time`=:time ON DUPLICATE KEY UPDATE `time`=:time;',
                                [
                                    ':characterid' => $this->id,
                                    ':achievementid' => $achievementid,
                                    ':time' => [$item['time'], 'datetime'],
                                ],
                            ];
                            $queries[] = [
                                'UPDATE `ffxiv__achievement` SET `earnedby`=`earnedby`+1 WHERE `achievementid`=:achievementid;',
                                [
                                    ':achievementid' => $achievementid,
                                ],
                            ];
                        }
                    }
                }
            }
            Query::query($queries);
            #Clean achievements, unless this is a new character
            if (!empty($updated)) {
                $this->cleanAchievements();
            }
            #Register the Free Company update if a change was detected
            if (!empty($this->lodestone['freeCompany']['id']) && !Query::query('SELECT `characterid` FROM `ffxiv__freecompany_character` WHERE `characterid`=:characterid AND `freecompanyid`=:fcID;', [':characterid' => $this->id, ':fcID' => $this->lodestone['freeCompany']['id']], return: 'check') && new FreeCompany($this->lodestone['freeCompany']['id'])->update() !== true) {
                new TaskInstance()->settingsFromArray(['task' => 'ffUpdateEntity', 'arguments' => [(string)$this->lodestone['freeCompany']['id'], 'freecompany'], 'message' => 'Updating free company with ID '.$this->lodestone['freeCompany']['id'], 'priority' => 1])->add();
            }
            #Register PvP Team update if a change was detected
            if (!empty($this->lodestone['pvp']['id']) && !Query::query('SELECT `characterid` FROM `ffxiv__pvpteam_character` WHERE `characterid`=:characterid AND `pvpteamid`=:pvpID;', [':characterid' => $this->id, ':pvpID' => $this->lodestone['pvp']['id']], return: 'check') && new PvPTeam($this->lodestone['pvp']['id'])->update() !== true) {
                new TaskInstance()->settingsFromArray(['task' => 'ffUpdateEntity', 'arguments' => [(string)$this->lodestone['pvp']['id'], 'pvpteam'], 'message' => 'Updating PvP team with ID '.$this->lodestone['pvp']['id'], 'priority' => 1])->add();
            }
            #Check if a character is linked to a user
            $character = Query::query('SELECT `characterid`, `userid` FROM `uc__user_to_ff_character` WHERE `characterid`=:id;', [':id' => $this->id], return: 'row');
            if (!empty($character['userid'])) {
                #Download avatar
                new User($character['userid'])->addAvatar(false, $this->lodestone['avatar'], (int)$this->id);
            }
            return true;
        } catch (\Throwable $e) {
            Errors::error_log($e, 'characterid: '.$this->id);
            return false;
        }
    }
    
    /**
     * Function to mark character as private
     * @return bool
     */
    protected function markPrivate(): bool
    {
        try {
            #In some cases we may have a server, name and avatar
            if (!empty($this->lodestone['server']) && !empty($this->lodestone['name']) && !empty($this->lodestone['avatar'])) {
                $queries = [];
                $queries[] = [
                    'UPDATE `ffxiv__character` SET `privated` = COALESCE(`privated`, UTC_DATE()), `updated`=CURRENT_TIMESTAMP() WHERE `characterid` = :characterid',
                    [
                        ':characterid' => $this->id,
                        ':server' => $this->lodestone['server'],
                        ':name' => $this->lodestone['name'],
                        ':avatar' => str_replace(['https://img2.finalfantasyxiv.com/f/', 'c0_96x96.jpg', 'c0.jpg'], '', $this->lodestone['avatar']),
                    ],
                ];
                $this->insertServerAndName($queries);
                return Query::query($queries);
            }
            $result = Query::query(
                'UPDATE `ffxiv__character` SET `privated` = COALESCE(`privated`, UTC_DATE()), `updated`=CURRENT_TIMESTAMP() WHERE `characterid` = :characterid',
                [':characterid' => $this->id],
            );
            #Also try cleaning achievements, but it does not matter much if it fails
            $this->cleanAchievements();
            return $result;
        } catch (\Throwable $e) {
            Errors::error_log($e, debug: $this->debug);
            return false;
        }
    }
    
    /**
     * Extracted function to update server and name of the character
     * @param array $queries
     *
     * @return void
     */
    private function insertServerAndName(array &$queries): void
    {
        #Insert server, if it has not been inserted yet. If the server is registered at all.
        if (Query::query('SELECT `serverid` FROM `ffxiv__server` WHERE `server`=:server;', [':server' => $this->lodestone['server']], return: 'check')) {
            $queries[] = [
                'INSERT IGNORE INTO `ffxiv__character_servers`(`characterid`, `serverid`) VALUES (:characterid, (SELECT `serverid` FROM `ffxiv__server` WHERE `server`=:server));',
                [
                    ':characterid' => $this->id,
                    ':server' => $this->lodestone['server'],
                ],
            ];
        }
        #Insert a name if it has not been inserted yet
        $queries[] = [
            'INSERT IGNORE INTO `ffxiv__character_names`(`characterid`, `name`) VALUES (:characterid, :name);',
            [
                ':characterid' => $this->id,
                ':name' => $this->lodestone['name'],
            ],
        ];
    }
    
    /**
     * Function to update the entity
     * @return bool
     */
    protected function delete(): bool
    {
        try {
            $queries = [];
            #Remove from Free Company
            $queries[] = [
                'UPDATE `ffxiv__freecompany_character` SET `current`=0 WHERE `characterid`=:characterid;',
                [
                    ':characterid' => $this->id,
                ],
            ];
            #Remove from PvP Team
            $queries[] = [
                'UPDATE `ffxiv__pvpteam_character` SET `current`=0 WHERE `characterid`=:characterid;',
                [
                    ':characterid' => $this->id,
                ],
            ];
            #Remove from Linkshells
            $queries[] = [
                'UPDATE `ffxiv__linkshell_character` SET `current`=0 WHERE `characterid`=:characterid;',
                [
                    ':characterid' => $this->id,
                ],
            ];
            #Update character
            $queries[] = [
                'UPDATE `ffxiv__character` SET `deleted` = COALESCE(`deleted`, UTC_DATE()), `updated`=CURRENT_TIMESTAMP() WHERE `characterid` = :id',
                [':id' => $this->id],
            ];
            $result = Query::query($queries);
            #Also try cleaning achievements, but it does not matter much if it fails
            $this->cleanAchievements();
            return $result;
        } catch (\Throwable $e) {
            Errors::error_log($e, debug: $this->debug);
            return false;
        }
    }
    
    /**
     * Link user to character
     * @return array
     */
    public function linkUser(): array
    {
        try {
            #Check if a character exists and is linked already
            $character = Query::query('SELECT `characterid`, `userid` FROM `uc__user_to_ff_character` WHERE `characterid`=:id;', [':id' => $this->id], return: 'row');
            if ($character['userid']) {
                return ['http_error' => 409, 'reason' => 'Character already linked'];
            }
            #Register or update the character
            $this->update();
            if (!empty($this->lodestone['id'])) {
                #Something went wrong with getting data
                if (!empty($this->lodestone['404'])) {
                    return ['http_error' => 400, 'reason' => 'No character found with id `'.$this->id.'`'];
                }
                return ['http_error' => 500, 'reason' => 'Failed to get fresh data for character with id `'.$this->id.'`'];
            }
            #Check if biography is set
            if (empty($this->lodestone['bio'])) {
                return ['http_error' => 424, 'reason' => 'No biography found for character with id `'.$this->id.'`'];
            }
            #Check if the biography contains the respected text
            $token = preg_replace('/(.*)(fftracker:([a-z\d]{64}))(.*)/uis', '$3', $this->lodestone['bio']);
            if (empty($token)) {
                return ['http_error' => 424, 'reason' => 'No tracker token found for character with id `'.$this->id.'`'];
            }
            #Check if the ID of the current user is the same as the user who has this token
            if (!Query::query('SELECT `userid` FROM `uc__users` WHERE `userid`=:userid AND `ff_token`=:token;', [':userid' => $_SESSION['userid'], ':token' => $token], return: 'check')) {
                return ['http_error' => 403, 'reason' => 'Wrong token or user provided'];
            }
            #Link character to user
            $result = Query::query([
                'INSERT IGNORE INTO `uc__user_to_ff_character` (`userid`, `characterid`) VALUES (:userid, :characterid);', [':userid' => $_SESSION['userid'], ':characterid' => $this->id],
                'INSERT IGNORE INTO `uc__user_to_group` (`userid`, `groupid`) VALUES (:userid, :groupid);', [':userid' => $_SESSION['userid'], ':groupid' => [Config::groupsIDs['Linked to FF'], 'int']],
            ]);
            Security::log('User details change', 'Attempted to link FFXIV character', ['id' => $this->id, 'result' => $result]);
            #Download avatar
            new User($_SESSION['userid'])->addAvatar(false, 'https://img2.finalfantasyxiv.com/f/'.$this->avatarID.'c0.jpg', $this->id);
            return ['response' => $result];
        } catch (\Throwable $exception) {
            return ['http_error' => 500, 'reason' => $exception->getMessage()];
        }
    }
    
    /**
     * Remove unnecessary achievements
     * @return bool
     */
    public function cleanAchievements(): bool
    {
        try {
            #Get achievements to remove
            $toRemove = Query::query(
            /** @lang MariaDB */ 'WITH `RankedAchievements` AS (
                        -- Rank achievements by time for characterid
                        SELECT
                            `fca`.`achievementid`,
                            `fca`.`characterid`,
                            `fa`.`dbid`,
                            `fca`.`time`,
                            ROW_NUMBER() OVER (PARTITION BY `fca`.characterid ORDER BY `fca`.time DESC) AS `rank`
                        FROM
                            `ffxiv__character_achievement` AS `fca`
                        JOIN
                            `ffxiv__achievement` AS `fa` ON `fca`.`achievementid` = `fa`.`achievementid`
                        WHERE
                            `fca`.`characterid` = :characterid
                    ),
                    `FilteredAchievements` AS (
                        -- Exclude the 50 latest achievements
                        SELECT *
                        FROM `RankedAchievements`
                        WHERE `rank` > 50
                    ),
                    `DbidNotNull` AS (
                        -- Select achievements with non-null dbid
                        SELECT *
                        FROM `FilteredAchievements`
                        WHERE `dbid` IS NOT NULL
                    ),
                    `DbidNull` AS (
                        -- Select achievements with null dbid
                        SELECT *
                        FROM `FilteredAchievements`
                        WHERE `dbid` IS NULL
                    ),
                    `LatestCharacters` AS (
                        -- Get up to 50 latest characters for each achievement in DbidNull
                        SELECT
                            `l`.`achievementid`,
                            `l`.`characterid`,
                            `l`.`time`
                        FROM (
                            SELECT
                                `fca`.`achievementid`,
                                `fca`.`characterid`,
                                `fca`.`time`,
                                ROW_NUMBER() OVER (PARTITION BY `fca`.`achievementid` ORDER BY `fca`.`time` DESC) AS `rank`
                            FROM
                                `ffxiv__character_achievement` AS `fca`
                            JOIN
                                `ffxiv__character` AS `fc` ON `fca`.`characterid` = `fc`.`characterid`
                            WHERE
                                `fca`.`achievementid` IN (SELECT `achievementid` FROM `DbidNull`)
                                AND `fc`.`deleted` IS NULL
                                AND `fc`.`privated` IS NULL
                        ) l
                        WHERE `l`.`rank` <= 50
                    ),
                    `FilteredLatestCharacters` AS (
                        -- Filter achievements to exclude those where characterid 6691027 is among the latest 50 characters
                        SELECT DISTINCT `l`.`achievementid`
                        FROM
                            `LatestCharacters` AS `l`
                        WHERE
                            `l`.`characterid` != :characterid
                    ),
                    `FinalDbidNullFiltered` AS (
                        -- Combine results from DbidNull and FilteredLatestCharacters
                        SELECT DISTINCT `dn`.*
                        FROM
                            `DbidNull` AS `dn`
                        WHERE
                            `dn`.`achievementid` IN (SELECT `achievementid` FROM `FilteredLatestCharacters`)
                    )
                    -- Combine results from DbidNotNull and FinalDbidNullFiltered
                    SELECT * FROM `DbidNotNull`
                    UNION ALL
                    SELECT * FROM `FinalDbidNullFiltered`;
                    ',
                [':characterid' => $this->id], return: 'column'
            );
            if (!empty($toRemove)) {
                Query::query('DELETE FROM `ffxiv__character_achievement` WHERE `characterid`=:characterid AND `achievementid` IN (:achievementid);',
                    [':characterid' => $this->id, ':achievementid' => [$toRemove, 'in', 'int']],
                );
            }
        } catch (\Throwable $exception) {
            Errors::error_log($exception);
        }
        return true;
    }
}