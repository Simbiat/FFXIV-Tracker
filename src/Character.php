<?php
declare(strict_types = 1);

namespace Simbiat\FFXIV;

use Simbiat\Cron\TaskInstance;
use Simbiat\Database\Query;
use Simbiat\Website\Config;
use Simbiat\Website\Errors;
use Simbiat\Website\HomePage;
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
    public ?string $avatar_id = '';
    public array $dates = [];
    public array $biology = [];
    public array $location = [];
    public ?string $biography = null;
    public array $title = [];
    public array $grand_company = [];
    public int $pvp = 0;
    public array $groups = [];
    public array $jobs = [];
    public array $achievements = [];
    public int $achievement_points = 0;
    public array $owned = [];
    
    /**
     * Function to get initial data from DB
     * @throws \Exception
     */
    protected function getFromDB(): array
    {
        #Get general information. Using *, but add name, because otherwise Achievement name overrides Character name, and we do not want that
        $data = Query::query('SELECT *, `ffxiv__character`.`character_id`, `ffxiv__achievement`.`icon` AS `title_icon`, `ffxiv__character`.`name`, `ffxiv__character`.`registered`, `ffxiv__character`.`updated` FROM `ffxiv__character`LEFT JOIN `uc__user_to_ff_character` ON `uc__user_to_ff_character`.`character_id`=`ffxiv__character`.`character_id` LEFT JOIN `ffxiv__clan` ON `ffxiv__character`.`clan_id` = `ffxiv__clan`.`clan_id` LEFT JOIN `ffxiv__guardian` ON `ffxiv__character`.`guardian_id` = `ffxiv__guardian`.`guardian_id` LEFT JOIN `ffxiv__nameday` ON `ffxiv__character`.`nameday_id` = `ffxiv__nameday`.`nameday_id` LEFT JOIN `ffxiv__city` ON `ffxiv__character`.`city_id` = `ffxiv__city`.`city_id` LEFT JOIN `ffxiv__server` ON `ffxiv__character`.`server_id` = `ffxiv__server`.`server_id` LEFT JOIN `ffxiv__grandcompany_rank` ON `ffxiv__character`.`gc_rank_id` = `ffxiv__grandcompany_rank`.`gc_rank_id` LEFT JOIN `ffxiv__grandcompany` ON `ffxiv__grandcompany_rank`.`gc_id` = `ffxiv__grandcompany`.`gc_id` LEFT JOIN `ffxiv__achievement` ON `ffxiv__character`.`title_id` = `ffxiv__achievement`.`achievement_id` WHERE `ffxiv__character`.`character_id` = :id;', [':id' => $this->id], return: 'row');
        if (!empty($data['hidden'])) {
            foreach ($data as $key => $value) {
                if (!\in_array($key, ['avatar', 'registered', 'updated', 'deleted', 'hidden', 'name'])) {
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
        if (!empty($data['user_id'])) {
            $data['username'] = Query::query('SELECT `username` FROM `uc__users` WHERE `user_id`=:user_id;', [':user_id' => $data['user_id']], return: 'value');
        } else {
            $data['username'] = null;
        }
        #Get jobs
        $data['jobs'] = Query::query('SELECT `name`, `level`, `last_change` FROM `ffxiv__character_jobs` LEFT JOIN `ffxiv__jobs` ON `ffxiv__character_jobs`.`job_id`=`ffxiv__jobs`.`job_id` WHERE `ffxiv__character_jobs`.`character_id`=:id ORDER BY `name`;', [':id' => $this->id], return: 'all');
        #Get old names. For now, returning only the count due to cases of bullying, when the old names are learnt. They are still being collected, though, for statistical purposes.
        $data['old_names'] = Query::query('SELECT `name` FROM `ffxiv__character_names` WHERE `character_id`=:id AND `name`!=:name', [':id' => $this->id, ':name' => $data['name']], return: 'column');
        #Get previous known incarnations (combination of gender and race/clan)
        $data['incarnations'] = Query::query('SELECT `gender`, `ffxiv__clan`.`race`, `ffxiv__clan`.`clan` FROM `ffxiv__character_clans` LEFT JOIN `ffxiv__clan` ON `ffxiv__character_clans`.`clan_id` = `ffxiv__clan`.`clan_id` WHERE `ffxiv__character_clans`.`character_id`=:id AND (`ffxiv__character_clans`.`clan_id`!=:clan_id AND `ffxiv__character_clans`.`gender`!=:gender) ORDER BY `gender` , `race` , `clan` ', [':id' => $this->id, ':clan_id' => $data['clan_id'], ':gender' => $data['gender']], return: 'all');
        #Get old servers
        $data['servers'] = Query::query('SELECT `ffxiv__server`.`data_center`, `ffxiv__server`.`server` FROM `ffxiv__character_servers` LEFT JOIN `ffxiv__server` ON `ffxiv__server`.`server_id`=`ffxiv__character_servers`.`server_id` WHERE `ffxiv__character_servers`.`character_id`=:id AND `ffxiv__character_servers`.`server_id` != :server_id ORDER BY `data_center` , `server` ', [':id' => $this->id, ':server_id' => $data['server_id']], return: 'all');
        #Get achievements
        $data['achievements'] = Query::query('SELECT \'achievement\' AS `type`, `ffxiv__achievement`.`achievement_id` AS `id`, `ffxiv__achievement`.`category`, `ffxiv__achievement`.`subcategory`, `ffxiv__achievement`.`name`, `time`, `icon` FROM `ffxiv__character_achievement` LEFT JOIN `ffxiv__achievement` ON `ffxiv__character_achievement`.`achievement_id`=`ffxiv__achievement`.`achievement_id` WHERE `ffxiv__character_achievement`.`character_id` = :id AND `ffxiv__achievement`.`category` IS NOT NULL AND `ffxiv__achievement`.`achievement_id` IS NOT NULL ORDER BY `time` DESC, `name` LIMIT 10', [':id' => $this->id], return: 'all');
        #Get affiliated groups' details
        $data['groups'] = AbstractTrackerEntity::cleanCrestResults(Query::query(
        /** @lang SQL */ '(SELECT \'freecompany\' AS `type`, 0 AS `crossworld`, `ffxiv__freecompany_character`.`fc_id` AS `id`, `ffxiv__freecompany`.`name` as `name`, `current`, `ffxiv__freecompany_character`.`rank_id`, `ffxiv__freecompany_rank`.`rankname` AS `rank`, `crest_part_1`, `crest_part_2`, `crest_part_3`, `gc_id` FROM `ffxiv__freecompany_character` LEFT JOIN `ffxiv__freecompany` ON `ffxiv__freecompany_character`.`fc_id`=`ffxiv__freecompany`.`fc_id` LEFT JOIN `ffxiv__freecompany_rank` ON `ffxiv__freecompany_rank`.`fc_id`=`ffxiv__freecompany`.`fc_id` AND `ffxiv__freecompany_character`.`rank_id`=`ffxiv__freecompany_rank`.`rank_id` WHERE `character_id`=:id)
            UNION ALL
            (SELECT \'linkshell\' AS `type`, `crossworld`, `ffxiv__linkshell_character`.`ls_id` AS `id`, `ffxiv__linkshell`.`name` as `name`, `current`, `ffxiv__linkshell_character`.`rank_id`, `ffxiv__linkshell_rank`.`rank` AS `rank`, null as `crest_part_1`, null as `crest_part_2`, null as `crest_part_3`, null as `gc_id` FROM `ffxiv__linkshell_character` LEFT JOIN `ffxiv__linkshell` ON `ffxiv__linkshell_character`.`ls_id`=`ffxiv__linkshell`.`ls_id` LEFT JOIN `ffxiv__linkshell_rank` ON `ffxiv__linkshell_character`.`rank_id`=`ffxiv__linkshell_rank`.`ls_rank_id` WHERE `character_id`=:id)
            UNION ALL
            (SELECT \'pvpteam\' AS `type`, 1 AS `crossworld`, `ffxiv__pvpteam_character`.`pvp_id` AS `id`, `ffxiv__pvpteam`.`name` as `name`, `current`, `ffxiv__pvpteam_character`.`rank_id`, `ffxiv__pvpteam_rank`.`rank` AS `rank`, `crest_part_1`, `crest_part_2`, `crest_part_3`, null as `gc_id` FROM `ffxiv__pvpteam_character` LEFT JOIN `ffxiv__pvpteam` ON `ffxiv__pvpteam_character`.`pvp_id`=`ffxiv__pvpteam`.`pvp_id` LEFT JOIN `ffxiv__pvpteam_rank` ON `ffxiv__pvpteam_character`.`rank_id`=`ffxiv__pvpteam_rank`.`pvp_rank_id` WHERE `character_id`=:id)
            ORDER BY `current` DESC, `name`;',
            [':id' => $this->id], return: 'all'
        ));
        #Clean up the data from unnecessary (technical) clutter
        unset($data['clan_id'], $data['nameday_id'], $data['achievement_id'], $data['category'], $data['subcategory'], $data['how_to'], $data['points'], $data['icon'], $data['item'], $data['item_icon'], $data['item_id'], $data['server_id']);
        #In case the entry is old enough (at least 1 day old) and register it for update. Also check that this is not a bot.
        if (empty(HomePage::$user_agent['bot']) && (\time() - \strtotime($data['updated'])) >= 86400) {
            new TaskInstance()->settingsFromArray(['task' => 'ff_update_entity', 'arguments' => [(string)$this->id, 'character'], 'message' => 'Updating character with ID '.$this->id, 'priority' => 1])->add();
        }
        return $data;
    }
    
    /**
     * Get character data from Lodestone
     *
     * @param bool $allow_sleep Whether to wait in case Lodestone throttles the request (that is throttle on our side)
     *
     * @return string|array
     */
    public function getFromLodestone(bool $allow_sleep = false): string|array
    {
        $lodestone = (new Lodestone());
        $data = $lodestone->getCharacter($this->id)->getCharacterJobs($this->id)->getResult();
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
            if (!empty($lodestone->getLastError()['error']) && \preg_match('/Lodestone has throttled the request, 429/', $lodestone->getLastError()['error']) === 1) {
                if ($allow_sleep) {
                    \sleep(60);
                }
                return 'Request throttled by Lodestone';
            }
            Errors::error_log(new \RuntimeException('Failed to get all necessary data for Character '.$this->id), $lodestone->getErrors());
            return 'Failed to get all necessary data for Character '.$this->id;
        }
        #Try to get achievements now, that we got basic information, and there were no issues with it.
        $data = $lodestone->getCharacterAchievements($this->id, false, 0, false, false, true)->getResult();
        $data = $data['characters'][$this->id];
        $data['id'] = $this->id;
        $data['404'] = false;
        return $data;
    }
    
    /**
     * Function to process data from DB
     *
     * @param array $from_db
     *
     * @return void
     */
    protected function process(array $from_db): void
    {
        $this->name = $from_db['name'];
        $this->avatar_id = $from_db['avatar'];
        $this->dates = [
            'registered' => \strtotime($from_db['registered']),
            'updated' => \strtotime($from_db['updated']),
            'hidden' => (empty($from_db['hidden']) ? null : \strtotime($from_db['hidden'])),
            'deleted' => (empty($from_db['deleted']) ? null : \strtotime($from_db['deleted'])),
        ];
        $this->biology = [
            'gender' => (int)($from_db['gender'] ?? 0),
            'race' => $from_db['race'] ?? null,
            'clan' => $from_db['clan'] ?? null,
            'nameday' => $from_db['nameday'] ?? null,
            'guardian' => $from_db['guardian'] ?? null,
            'guardian_id' => $from_db['guardian_id'] ?? null,
            'incarnations' => $from_db['incarnations'] ?? null,
            'old_names' => $from_db['old_names'] ?? [],
        ];
        $this->location = [
            'data_center' => $from_db['data_center'] ?? null,
            'server' => $from_db['server'] ?? null,
            'region' => $from_db['region'] ?? null,
            'city' => $from_db['city'] ?? null,
            'city_id' => $from_db['city_id'] ?? null,
            'previous_servers' => $from_db['servers'] ?? [],
        ];
        $this->biography = $from_db['biography'] ?? null;
        $this->title = [
            'title' => $from_db['title'] ?? null,
            'icon' => $from_db['title_icon'] ?? null,
            'id' => $from_db['title_id'] ?? null,
        ];
        $this->grand_company = [
            'name' => $from_db['gc_name'] ?? null,
            'rank' => $from_db['gc_rank'] ?? null,
            'gc_id' => $from_db['gc_id'] ?? null,
            'gc_rank_id' => $from_db['gc_rank_id'] ?? null,
        ];
        $this->pvp = (int)($from_db['pvp_matches'] ?? 0);
        $this->groups = $from_db['groups'] ?? [];
        $this->owned = [
            'id' => $from_db['user_id'] ?? null,
            'name' => $from_db['username'] ?? null
        ];
        foreach ($this->groups as $key => $group) {
            $this->groups[$key]['current'] = (bool)$group['current'];
        }
        $this->achievements = $from_db['achievements'] ?? [];
        foreach ($this->achievements as $key => $achievement) {
            $this->achievements[$key]['time'] = (empty($achievement['time']) ? null : \strtotime($achievement['time']));
        }
        $this->achievement_points = $from_db['achievement_points'] ?? 0;
        $this->jobs = $from_db['jobs'] ?? [];
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
            $updated = Query::query('SELECT `updated` FROM `ffxiv__character` WHERE `character_id`=:character_id', [':character_id' => $this->id], return: 'value');
            #If a character on Lodestone is not registered in Free Company or PvP Team, add their IDs as NULL for consistency
            if (empty($this->lodestone['free_company']['id'])) {
                $this->lodestone['free_company']['id'] = NULL;
                $this->lodestone['free_company']['registered'] = false;
            } else {
                $this->lodestone['free_company']['registered'] = Query::query('SELECT `fc_id` FROM `ffxiv__freecompany` WHERE `fc_id` = :id', [':id' => $this->lodestone['free_company']['id']], return: 'check');
            }
            if (empty($this->lodestone['pvp']['id'])) {
                $this->lodestone['pvp']['id'] = NULL;
                $this->lodestone['pvp']['registered'] = false;
            } else {
                $this->lodestone['pvp']['registered'] = Query::query('SELECT `pvp_id` FROM `ffxiv__pvpteam` WHERE `pvp_id` = :id', [':id' => $this->lodestone['pvp']['id']], return: 'check');
            }
            #Insert Free Companies and PvP Team if they are not registered
            if ($this->lodestone['free_company']['id'] !== NULL && $this->lodestone['free_company']['registered'] === false) {
                $queries[] = [
                    'INSERT IGNORE INTO `ffxiv__freecompany` (`fc_id`, `name`, `server_id`, `updated`) VALUES (:fc_id, :fc_name, (SELECT `server_id` FROM `ffxiv__server` WHERE `server`=:server), TIMESTAMPADD(SECOND, -3600, CURRENT_TIMESTAMP()));',
                    [
                        ':fc_id' => $this->lodestone['free_company']['id'],
                        ':fc_name' => $this->lodestone['free_company']['name'],
                        ':server' => $this->lodestone['server'],
                    ],
                ];
            }
            if ($this->lodestone['pvp']['id'] !== NULL && $this->lodestone['pvp']['registered'] === false) {
                $queries[] = [
                    'INSERT IGNORE INTO `ffxiv__pvpteam` (`pvp_id`, `name`, `data_center_id`, `updated`) VALUES (:pvp_id, :pvp_name, (SELECT `server_id` FROM `ffxiv__server` WHERE `server`=:server), TIMESTAMPADD(SECOND, -3600, CURRENT_TIMESTAMP()));',
                    [
                        ':pvp_id' => $this->lodestone['pvp']['id'],
                        ':pvp_name' => $this->lodestone['pvp']['name'],
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
            $achievement_points = 0;
            if (!empty($this->lodestone['achievements']) && is_array($this->lodestone['achievements'])) {
                foreach ($this->lodestone['achievements'] as $item) {
                    $achievement_points += (int)$item['points'];
                }
            }
            #Main query to insert or update a character
            $queries[] = [
                'INSERT INTO `ffxiv__character`(
                    `character_id`, `server_id`, `name`, `registered`, `updated`, `hidden`, `deleted`, `biography`, `title_id`, `avatar`, `clan_id`, `gender`, `nameday_id`, `guardian_id`, `city_id`, `gc_rank_id`, `pvp_matches`, `achievement_points`
                )
                VALUES (
                    :character_id, (SELECT `server_id` FROM `ffxiv__server` WHERE `server`=:server), :name, UTC_DATE(), CURRENT_TIMESTAMP(), NULL, NULL, :biography, (SELECT `achievement_id` as `title_id` FROM `ffxiv__achievement` WHERE `title` IS NOT NULL AND `title`=:title LIMIT 1), :avatar, (SELECT `clan_id` FROM `ffxiv__clan` WHERE `clan`=:clan), :gender, (SELECT `nameday_id` FROM `ffxiv__nameday` WHERE `nameday`=:nameday), (SELECT `guardian_id` FROM `ffxiv__guardian` WHERE `guardian`=:guardian), (SELECT `city_id` FROM `ffxiv__city` WHERE `city`=:city), `gc_rank_id` = (SELECT `gc_rank_id` FROM `ffxiv__grandcompany_rank` WHERE `gc_rank`=:gcRank ORDER BY `gc_rank_id` LIMIT 1), 0, :achievement_points
                )
                ON DUPLICATE KEY UPDATE
                    `server_id`=(SELECT `server_id` FROM `ffxiv__server` WHERE `server`=:server), `name`=:name, `updated`=CURRENT_TIMESTAMP(), `hidden`=NULL, `deleted`=NULL, `biography`=:biography, `title_id`=(SELECT `achievement_id` as `title_id` FROM `ffxiv__achievement` WHERE `title` IS NOT NULL AND `title`=:title LIMIT 1), `avatar`=:avatar, `clan_id`=(SELECT `clan_id` FROM `ffxiv__clan` WHERE `clan`=:clan), `gender`=:gender, `nameday_id`=(SELECT `nameday_id` FROM `ffxiv__nameday` WHERE `nameday`=:nameday), `guardian_id`=(SELECT `guardian_id` FROM `ffxiv__guardian` WHERE `guardian`=:guardian), `city_id`=(SELECT `city_id` FROM `ffxiv__city` WHERE `city`=:city), `gc_rank_id`=(SELECT `gc_rank_id` FROM `ffxiv__grandcompany_rank` WHERE `gc_rank` IS NOT NULL AND `gc_rank`=:gcRank ORDER BY `gc_rank_id` LIMIT 1), `achievement_points`=:achievement_points;',
                [
                    ':character_id' => $this->id,
                    ':server' => $this->lodestone['server'],
                    ':name' => $this->lodestone['name'],
                    ':avatar' => \str_replace(['https://img2.finalfantasyxiv.com/f/', 'c0_96x96.jpg', 'c0.jpg'], '', $this->lodestone['avatar']),
                    ':biography' => [
                        (empty($this->lodestone['bio']) ? NULL : $this->lodestone['bio']),
                        (empty($this->lodestone['bio']) ? 'null' : 'string'),
                    ],
                    ':title' => (empty($this->lodestone['title']) ? '' : $this->lodestone['title']),
                    ':clan' => $this->lodestone['clan'],
                    ':gender' => ($this->lodestone['gender'] === 'male' ? '1' : '0'),
                    ':nameday' => $this->lodestone['nameday'],
                    ':guardian' => $this->lodestone['guardian']['name'],
                    ':city' => $this->lodestone['city']['name'],
                    ':gcRank' => (empty($this->lodestone['grand_company']['rank']) ? '' : $this->lodestone['grand_company']['rank']),
                    ':achievement_points' => [$achievement_points, 'int']
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
                            'INSERT INTO `ffxiv__character_jobs` (`character_id`, `job_id`, `level`, `last_change`) VALUES (:character_id, (SELECT `job_id` FROM `ffxiv__jobs` WHERE `name`=:jobname), :level, CURRENT_TIMESTAMP()) ON DUPLICATE KEY UPDATE `level`=:level, `last_change`=IF(`level`=:level, `last_change`, CURRENT_TIMESTAMP());',
                            [
                                ':character_id' => $this->id,
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
                    'INSERT IGNORE INTO `ffxiv__character_clans`(`character_id`, `gender`, `clan_id`) VALUES (:character_id, :gender, (SELECT `clan_id` FROM `ffxiv__clan` WHERE `clan`=:clan));',
                    [
                        ':character_id' => $this->id,
                        ':gender' => ($this->lodestone['gender'] === 'male' ? '1' : '0'),
                        ':clan' => $this->lodestone['clan'],
                    ],
                ];
            }
            #Update company information
            $queries[] = [
                'UPDATE `ffxiv__freecompany_character` SET `current`=0 WHERE `character_id`=:character_id AND `fc_id` '.(empty($this->lodestone['free_company']['id']) ? 'IS NOT ' : '!= ').' :fc_id;',
                [
                    ':character_id' => $this->id,
                    ':fc_id' => [
                        $this->lodestone['free_company']['id'],
                        (empty($this->lodestone['free_company']['id']) ? 'null' : 'string'),
                    ],
                ],
            ];
            #Update PvP Team information
            $queries[] = [
                'UPDATE `ffxiv__pvpteam_character` SET `current`=0 WHERE `character_id`=:character_id AND `pvp_id` '.(empty($this->lodestone['pvp']['id']) ? 'IS NOT ' : '!= ').' :pvp_id;',
                [
                    ':character_id' => $this->id,
                    ':pvp_id' => [
                        $this->lodestone['pvp']['id'],
                        (empty($this->lodestone['pvp']['id']) ? 'null' : 'string'),
                    ],
                ],
            ];
            #Achievements
            if (!empty($this->lodestone['achievements']) && is_array($this->lodestone['achievements'])) {
                foreach ($this->lodestone['achievements'] as $achievement_id => $item) {
                    $icon = self::removeLodestoneDomain($item['icon']);
                    #Download the icon if it's not already present
                    if (\is_file(\str_replace('.png', '.webp', Config::$icons.$icon))) {
                        $webp = true;
                    } else {
                        $webp = Images::download($item['icon'], Config::$icons.$icon);
                    }
                    if ($webp) {
                        $icon = \str_replace('.png', '.webp', $icon);
                        $queries[] = [
                            'INSERT INTO `ffxiv__achievement` SET `achievement_id`=:achievement_id, `name`=:name, `icon`=:icon, `points`=:points ON DUPLICATE KEY UPDATE `updated`=`updated`, `name`=:name, `icon`=:icon, `points`=:points;',
                            [
                                ':achievement_id' => $achievement_id,
                                ':name' => $item['name'],
                                ':icon' => $icon,
                                ':points' => $item['points'],
                            ],
                        ];
                        #If the achievement is new since the last check, or if this is the first time the character is being processed, add and count the achievement
                        if (!empty($updated) && (int)$item['time'] > \strtotime($updated)) {
                            $queries[] = [
                                'INSERT INTO `ffxiv__character_achievement` SET `character_id`=:character_id, `achievement_id`=:achievement_id, `time`=:time ON DUPLICATE KEY UPDATE `time`=:time;',
                                [
                                    ':character_id' => $this->id,
                                    ':achievement_id' => $achievement_id,
                                    ':time' => [$item['time'], 'datetime'],
                                ],
                            ];
                            $queries[] = [
                                'UPDATE `ffxiv__achievement` SET `earned_by`=`earned_by`+1 WHERE `achievement_id`=:achievement_id;',
                                [
                                    ':achievement_id' => $achievement_id,
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
            if (!empty($this->lodestone['free_company']['id']) && !Query::query('SELECT `character_id` FROM `ffxiv__freecompany_character` WHERE `character_id`=:character_id AND `fc_id`=:fcID;', [':character_id' => $this->id, ':fcID' => $this->lodestone['free_company']['id']], return: 'check') && new FreeCompany($this->lodestone['free_company']['id'])->update() !== true) {
                new TaskInstance()->settingsFromArray(['task' => 'ff_update_entity', 'arguments' => [(string)$this->lodestone['free_company']['id'], 'freecompany'], 'message' => 'Updating free company with ID '.$this->lodestone['free_company']['id'], 'priority' => 1])->add();
            }
            #Register PvP Team update if a change was detected
            if (!empty($this->lodestone['pvp']['id']) && !Query::query('SELECT `character_id` FROM `ffxiv__pvpteam_character` WHERE `character_id`=:character_id AND `pvp_id`=:pvpID;', [':character_id' => $this->id, ':pvpID' => $this->lodestone['pvp']['id']], return: 'check') && new PvPTeam($this->lodestone['pvp']['id'])->update() !== true) {
                new TaskInstance()->settingsFromArray(['task' => 'ff_update_entity', 'arguments' => [(string)$this->lodestone['pvp']['id'], 'pvpteam'], 'message' => 'Updating PvP team with ID '.$this->lodestone['pvp']['id'], 'priority' => 1])->add();
            }
            #Check if a character is linked to a user
            $character = Query::query('SELECT `character_id`, `user_id` FROM `uc__user_to_ff_character` WHERE `character_id`=:id;', [':id' => $this->id], return: 'row');
            if (!empty($character['user_id'])) {
                #Download avatar
                new User($character['user_id'])->addAvatar(false, $this->lodestone['avatar'], (int)$this->id);
            }
            return true;
        } catch (\Throwable $exception) {
            Errors::error_log($exception, 'character_id: '.$this->id);
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
            #In some cases, we may have a server, name and avatar
            if (!empty($this->lodestone['server']) && !empty($this->lodestone['name']) && !empty($this->lodestone['avatar'])) {
                $queries = [];
                $queries[] = [
                    'UPDATE `ffxiv__character` SET `hidden` = COALESCE(`hidden`, UTC_DATE()), `updated`=CURRENT_TIMESTAMP() WHERE `character_id` = :character_id',
                    [
                        ':character_id' => $this->id,
                        ':server' => $this->lodestone['server'],
                        ':name' => $this->lodestone['name'],
                        ':avatar' => \str_replace(['https://img2.finalfantasyxiv.com/f/', 'c0_96x96.jpg', 'c0.jpg'], '', $this->lodestone['avatar']),
                    ],
                ];
                $this->insertServerAndName($queries);
                return Query::query($queries);
            }
            $result = Query::query(
                'UPDATE `ffxiv__character` SET `hidden` = COALESCE(`hidden`, UTC_DATE()), `updated`=CURRENT_TIMESTAMP() WHERE `character_id` = :character_id',
                [':character_id' => $this->id],
            );
            #Also try cleaning achievements, but it does not matter much if it fails
            $this->cleanAchievements();
            return $result;
        } catch (\Throwable $exception) {
            Errors::error_log($exception, debug: $this->debug);
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
        if (Query::query('SELECT `server_id` FROM `ffxiv__server` WHERE `server`=:server;', [':server' => $this->lodestone['server']], return: 'check')) {
            $queries[] = [
                'INSERT IGNORE INTO `ffxiv__character_servers`(`character_id`, `server_id`) VALUES (:character_id, (SELECT `server_id` FROM `ffxiv__server` WHERE `server`=:server));',
                [
                    ':character_id' => $this->id,
                    ':server' => $this->lodestone['server'],
                ],
            ];
        }
        #Insert a name if it has not been inserted yet
        $queries[] = [
            'INSERT IGNORE INTO `ffxiv__character_names`(`character_id`, `name`) VALUES (:character_id, :name);',
            [
                ':character_id' => $this->id,
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
                'UPDATE `ffxiv__freecompany_character` SET `current`=0 WHERE `character_id`=:character_id;',
                [
                    ':character_id' => $this->id,
                ],
            ];
            #Remove from PvP Team
            $queries[] = [
                'UPDATE `ffxiv__pvpteam_character` SET `current`=0 WHERE `character_id`=:character_id;',
                [
                    ':character_id' => $this->id,
                ],
            ];
            #Remove from Linkshells
            $queries[] = [
                'UPDATE `ffxiv__linkshell_character` SET `current`=0 WHERE `character_id`=:character_id;',
                [
                    ':character_id' => $this->id,
                ],
            ];
            #Update character
            $queries[] = [
                'UPDATE `ffxiv__character` SET `deleted` = COALESCE(`deleted`, UTC_DATE()), `updated`=CURRENT_TIMESTAMP() WHERE `character_id` = :id',
                [':id' => $this->id],
            ];
            $result = Query::query($queries);
            #Also try cleaning achievements, but it does not matter much if it fails
            $this->cleanAchievements();
            return $result;
        } catch (\Throwable $exception) {
            Errors::error_log($exception, debug: $this->debug);
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
            $character = Query::query('SELECT `character_id`, `user_id` FROM `uc__user_to_ff_character` WHERE `character_id`=:id;', [':id' => $this->id], return: 'row');
            if ($character['user_id']) {
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
            $token = \preg_replace('/(.*)(fftracker:([a-z\d]{64}))(.*)/uis', '$3', $this->lodestone['bio']);
            if (empty($token)) {
                return ['http_error' => 424, 'reason' => 'No tracker token found for character with id `'.$this->id.'`'];
            }
            #Check if the ID of the current user is the same as the user who has this token
            if (!Query::query('SELECT `user_id` FROM `uc__users` WHERE `user_id`=:user_id AND `ff_token`=:token;', [':user_id' => $_SESSION['user_id'], ':token' => $token], return: 'check')) {
                return ['http_error' => 403, 'reason' => 'Wrong token or user provided'];
            }
            #Link character to user
            $result = Query::query([
                'INSERT IGNORE INTO `uc__user_to_ff_character` (`user_id`, `character_id`) VALUES (:user_id, :character_id);', [':user_id' => $_SESSION['user_id'], ':character_id' => $this->id],
                'INSERT IGNORE INTO `uc__user_to_group` (`user_id`, `group_id`) VALUES (:user_id, :group_id);', [':user_id' => $_SESSION['user_id'], ':group_id' => [Config::GROUP_IDS['Linked to FF'], 'int']],
            ]);
            Security::log('User details change', 'Attempted to link FFXIV character', ['id' => $this->id, 'result' => $result]);
            #Download avatar
            new User($_SESSION['user_id'])->addAvatar(false, 'https://img2.finalfantasyxiv.com/f/'.$this->avatar_id.'c0.jpg', $this->id);
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
            $to_remove = Query::query(
            /** @lang MariaDB */ 'WITH `RankedAchievements` AS (
                        -- Rank achievements by time for character_id
                        SELECT
                            `fca`.`achievement_id`,
                            `fca`.`character_id`,
                            `fa`.`db_id`,
                            `fca`.`time`,
                            ROW_NUMBER() OVER (PARTITION BY `fca`.`character_id` ORDER BY `fca`.time DESC) AS `rank`
                        FROM
                            `ffxiv__character_achievement` AS `fca`
                        JOIN
                            `ffxiv__achievement` AS `fa` ON `fca`.`achievement_id` = `fa`.`achievement_id`
                        WHERE
                            `fca`.`character_id` = :character_id
                    ),
                    `FilteredAchievements` AS (
                        -- Exclude the 50 latest achievements
                        SELECT *
                        FROM `RankedAchievements`
                        WHERE `rank` > 50
                    ),
                    `DbidNotNull` AS (
                        -- Select achievements with non-null db_id
                        SELECT *
                        FROM `FilteredAchievements`
                        WHERE `db_id` IS NOT NULL
                    ),
                    `DbidNull` AS (
                        -- Select achievements with null db_id
                        SELECT *
                        FROM `FilteredAchievements`
                        WHERE `db_id` IS NULL
                    ),
                    `LatestCharacters` AS (
                        -- Get up to 50 latest characters for each achievement in DbidNull
                        SELECT
                            `l`.`achievement_id`,
                            `l`.`character_id`,
                            `l`.`time`
                        FROM (
                            SELECT
                                `fca`.`achievement_id`,
                                `fca`.`character_id`,
                                `fca`.`time`,
                                ROW_NUMBER() OVER (PARTITION BY `fca`.`achievement_id` ORDER BY `fca`.`time` DESC) AS `rank`
                            FROM
                                `ffxiv__character_achievement` AS `fca`
                            JOIN
                                `ffxiv__character` AS `fc` ON `fca`.`character_id` = `fc`.`character_id`
                            WHERE
                                `fca`.`achievement_id` IN (SELECT `achievement_id` FROM `DbidNull`)
                                AND `fc`.`deleted` IS NULL
                                AND `fc`.`hidden` IS NULL
                        ) l
                        WHERE `l`.`rank` <= 50
                    ),
                    `FilteredLatestCharacters` AS (
                        -- Filter achievements to exclude those where character_id 6691027 is among the latest 50 characters
                        SELECT DISTINCT `l`.`achievement_id`
                        FROM
                            `LatestCharacters` AS `l`
                        WHERE
                            `l`.`character_id` != :character_id
                    ),
                    `FinalDbidNullFiltered` AS (
                        -- Combine results from DbidNull and FilteredLatestCharacters
                        SELECT DISTINCT `dn`.*
                        FROM
                            `DbidNull` AS `dn`
                        WHERE
                            `dn`.`achievement_id` IN (SELECT `achievement_id` FROM `FilteredLatestCharacters`)
                    )
                    -- Combine results from DbidNotNull and FinalDbidNullFiltered
                    SELECT * FROM `DbidNotNull`
                    UNION ALL
                    SELECT * FROM `FinalDbidNullFiltered`;
                    ',
                [':character_id' => $this->id], return: 'column'
            );
            if (!empty($to_remove)) {
                Query::query('DELETE FROM `ffxiv__character_achievement` WHERE `character_id`=:character_id AND `achievement_id` IN (:achievement_id);',
                    [':character_id' => $this->id, ':achievement_id' => [$to_remove, 'in', 'int']],
                );
            }
        } catch (\Throwable $exception) {
            Errors::error_log($exception);
        }
        return true;
    }
}