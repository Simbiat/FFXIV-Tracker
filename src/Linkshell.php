<?php
declare(strict_types = 1);

namespace Simbiat\FFXIV;

use Simbiat\Cron\TaskInstance;
use Simbiat\Database\Query;
use Simbiat\Website\Errors;

/**
 * Class representing a FFXIV linkshell (chat group)
 */
class Linkshell extends AbstractTrackerEntity
{
    #Custom properties
    protected const string ENTITY_TYPE = 'linkshell';
    protected const bool CROSSWORLD = false;
    public array $dates = [];
    public ?string $community = null;
    public ?string $server = null;
    public ?string $data_center = null;
    public array $old_names = [];
    public array $members = [];
    
    /**Function to get initial data from DB
     * @throws \Exception
     */
    protected function getFromDB(): array
    {
        #Get general information
        $data = Query::query('SELECT * FROM `ffxiv__linkshell` LEFT JOIN `ffxiv__server` ON `ffxiv__linkshell`.`server_id`=`ffxiv__server`.`server_id` WHERE `ls_id`=:id', [':id' => $this->id], return: 'row');
        #Return empty if nothing was found
        if (empty($data)) {
            return [];
        }
        #Get old names
        $data['old_names'] = Query::query('SELECT `name` FROM `ffxiv__linkshell_names` WHERE `ls_id`=:id AND `name`<>:name', [':id' => $this->id, ':name' => $data['name']], return: 'column');
        #Get members
        $data['members'] = Query::query('SELECT \'character\' AS `type`, `ffxiv__linkshell_character`.`character_id` AS `id`, `ffxiv__character`.`name`, `ffxiv__character`.`avatar` AS `icon`, `ffxiv__linkshell_rank`.`rank`, `ffxiv__linkshell_rank`.`ls_rank_id`, `user_id` FROM `ffxiv__linkshell_character` LEFT JOIN `uc__user_to_ff_character` ON `uc__user_to_ff_character`.`character_id`=`ffxiv__linkshell_character`.`character_id` LEFT JOIN `ffxiv__linkshell_rank` ON `ffxiv__linkshell_rank`.`ls_rank_id`=`ffxiv__linkshell_character`.`rank_id` LEFT JOIN `ffxiv__character` ON `ffxiv__linkshell_character`.`character_id`=`ffxiv__character`.`character_id` WHERE `ffxiv__linkshell_character`.`ls_id`=:id AND `current`=1 ORDER BY `ffxiv__linkshell_character`.`rank_id` , `ffxiv__character`.`name` ', [':id' => $this->id], return: 'all');
        #Clean up the data from unnecessary (technical) clutter
        unset($data['server_id']);
        if ($data['crossworld']) {
            unset($data['server']);
        }
        #In case the entry is old enough (at least 1 day old) and register it for update. Also check that this is not a bot.
        if (empty($_SESSION['useragent']['bot']) && (\time() - \strtotime($data['updated'])) >= 86400) {
            if ((int)$data['crossworld'] === 0) {
                new TaskInstance()->settingsFromArray(['task' => 'ff_update_entity', 'arguments' => [(string)$this->id, 'linkshell'], 'message' => 'Updating linkshell with ID '.$this->id, 'priority' => 1])->add();
            } else {
                new TaskInstance()->settingsFromArray(['task' => 'ff_update_entity', 'arguments' => [(string)$this->id, 'crossworldlinkshell'], 'message' => 'Updating crossworld linkshell with ID '.$this->id, 'priority' => 1])->add();
            }
        }
        return $data;
    }
    
    /**
     * Get linkshell data from Lodestone
     *
     * @param bool $allow_sleep Whether to wait in case Lodestone throttles the request (that is throttle on our side)
     *
     * @return string|array
     */
    public function getFromLodestone(bool $allow_sleep = false): string|array
    {
        $lodestone = (new Lodestone());
        $data = $lodestone->getLinkshellMembers($this->id, 0)->getResult();
        if (empty($data['linkshells']) || empty($data['linkshells'][$this->id]['server']) || (empty($data['linkshells'][$this->id]['members']) && (int)$data['linkshells'][$this->id]['members_count'] > 0) || (!empty($data['linkshells'][$this->id]['members']) && \count($data['linkshells'][$this->id]['members']) < (int)$data['linkshells'][$this->id]['members_count'])) {
            if (!empty($data['linkshells'][$this->id]['members']) && $data['linkshells'][$this->id]['members'] === 404) {
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
            if (empty($data['linkshells']) || empty($data['linkshells'][$this->id]) || !isset($data['linkshells'][$this->id]['page_total']) || $data['linkshells'][$this->id]['page_total'] !== 0) {
                if (empty($lodestone->getLastError())) {
                    return 'Failed to get any data for '.($this::CROSSWORLD ? 'Crossworld ' : '').'Linkshell '.$this->id;
                }
                return 'Failed to get all necessary data for '.($this::CROSSWORLD ? 'Crossworld ' : '').'Linkshell '.$this->id.' ('.$lodestone->getLastError()['url'].'): '.$lodestone->getLastError()['error'];
            }
            #At some point, empty linkshells became possible on lodestone, those that have a page, but no members at all, and are not searchable by name. Possibly private linkshells or something like that
            $data['linkshells'][$this->id]['empty'] = true;
        }
        $data = $data['linkshells'][$this->id];
        $data['id'] = $this->id;
        $data['404'] = false;
        unset($data['page_current'], $data['page_total']);
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
        $this->community = $from_db['community_id'];
        $this->dates = [
            'formed' => (empty($from_db['formed']) ? null : \strtotime($from_db['formed'])),
            'registered' => \strtotime($from_db['registered']),
            'updated' => \strtotime($from_db['updated']),
            'deleted' => (empty($from_db['deleted']) ? null : \strtotime($from_db['deleted'])),
        ];
        $this->old_names = $from_db['old_names'];
        $this->members = $from_db['members'];
        if ($this::CROSSWORLD) {
            $this->data_center = $from_db['data_center'];
        } else {
            $this->server = $from_db['server'];
        }
    }
    
    /**
     * Function to update the entity
     *
     * @return bool
     */
    protected function updateDB(): bool
    {
        try {
            #If the `empty` flag is set, it means that the Lodestone page is empty, so we can't update anything besides name, data center and formed date
            if (isset($this->lodestone['empty']) && $this->lodestone['empty'] === true) {
                $queries[] = [
                    'UPDATE `ffxiv__linkshell` SET `name`=:name, `formed`=:formed, `updated`=CURRENT_TIMESTAMP(), `deleted`=NULL WHERE `ls_id`=:ls_id',
                    [
                        ':ls_id' => $this->id,
                        ':name' => $this->lodestone['name'],
                        ':crossworld' => [$this::CROSSWORLD, 'bool'],
                        ':formed' => [
                            (empty($this->lodestone['formed']) ? null : $this->lodestone['formed']),
                            (empty($this->lodestone['formed']) ? 'null' : 'date'),
                        ],
                    ],
                ];
            } else {
                #Main query to insert or update a Linkshell
                $queries[] = [
                    'INSERT INTO `ffxiv__linkshell`(`ls_id`, `name`, `crossworld`, `formed`, `registered`, `updated`, `deleted`, `server_id`, `community_id`) VALUES (:ls_id, :name, :crossworld, :formed, UTC_DATE(), CURRENT_TIMESTAMP(), NULL, (SELECT `server_id` FROM `ffxiv__server` WHERE `server`=:server OR `data_center`=:server ORDER BY `server_id` LIMIT 1), :community_id) ON DUPLICATE KEY UPDATE `name`=:name, `formed`=:formed, `updated`=CURRENT_TIMESTAMP(), `deleted`=NULL, `server_id`=(SELECT `server_id` FROM `ffxiv__server` WHERE `server`=:server OR `data_center`=:server ORDER BY `server_id` LIMIT 1), `community_id`=:community_id;',
                    [
                        ':ls_id' => $this->id,
                        ':server' => $this->lodestone['server'] ?? $this->lodestone['data_center'],
                        ':name' => $this->lodestone['name'],
                        ':crossworld' => [$this::CROSSWORLD, 'bool'],
                        ':formed' => [
                            (empty($this->lodestone['formed']) ? null : $this->lodestone['formed']),
                            (empty($this->lodestone['formed']) ? 'null' : 'date'),
                        ],
                        ':community_id' => [
                            (empty($this->lodestone['community_id']) ? null : $this->lodestone['community_id']),
                            (empty($this->lodestone['community_id']) ? 'null' : 'string'),
                        ],
                    ],
                ];
            }
            #Register Linkshell name if it's not registered already
            $queries[] = [
                'INSERT IGNORE INTO `ffxiv__linkshell_names`(`ls_id`, `name`) VALUES (:ls_id, :name);',
                [
                    ':ls_id' => $this->id,
                    ':name' => $this->lodestone['name'],
                ],
            ];
            #Get members as registered on the tracker
            $track_members = Query::query('SELECT `character_id` FROM `ffxiv__linkshell_character` WHERE `ls_id`=:ls_id AND `current`=1;', [':ls_id' => $this->id], return: 'column');
            #Process members that left the linkshell
            foreach ($track_members as $member) {
                #Check if member from tracker is present in a Lodestone list
                if (!isset($this->lodestone['members'][$member])) {
                    #Update status for the character
                    $queries[] = [
                        'UPDATE `ffxiv__linkshell_character` SET `current`=0 WHERE `ls_id`=:ls_id AND `character_id`=:character_id;',
                        [
                            ':character_id' => $member,
                            ':ls_id' => $this->id,
                        ],
                    ];
                }
            }
            #Process Lodestone members
            if (!empty($this->lodestone['members'])) {
                foreach ($this->lodestone['members'] as $member => $details) {
                    #Check if a member is registered on the tracker while saving the status for future use
                    $this->lodestone['members'][$member]['registered'] = Query::query('SELECT `character_id` FROM `ffxiv__character` WHERE `character_id`=:character_id', [':character_id' => $member], return: 'check');
                    if (!$this->lodestone['members'][$member]['registered']) {
                        #Create a basic entry of the character
                        $queries[] = [
                            'INSERT IGNORE INTO `ffxiv__character`(
                            `character_id`, `server_id`, `name`, `registered`, `updated`, `avatar`, `gc_rank_id`
                        )
                        VALUES (
                            :character_id, (SELECT `server_id` FROM `ffxiv__server` WHERE `server`=:server), :name, UTC_DATE(), TIMESTAMPADD(SECOND, -3600, CURRENT_TIMESTAMP()), :avatar, `gc_rank_id` = (SELECT `gc_rank_id` FROM `ffxiv__grandcompany_rank` WHERE `gc_rank`=:gcRank ORDER BY `gc_rank_id` LIMIT 1)
                        ) ON DUPLICATE KEY UPDATE `deleted`=NULL;',
                            [
                                ':character_id' => $member,
                                ':server' => $details['server'],
                                ':name' => $details['name'],
                                ':avatar' => \str_replace(['https://img2.finalfantasyxiv.com/f/', 'c0.jpg'], '', $details['avatar']),
                                ':gcRank' => (empty($details['grand_company']['rank']) ? '' : $details['grand_company']['rank']),
                            ]
                        ];
                    }
                    #Insert/update character relationship with linkshell
                    $queries[] = [
                        'INSERT INTO `ffxiv__linkshell_character` (`ls_id`, `character_id`, `rank_id`, `current`) VALUES (:ls_id, :member_id, (SELECT `ls_rank_id` FROM `ffxiv__linkshell_rank` WHERE `rank`=:rank LIMIT 1), 1) ON DUPLICATE KEY UPDATE `rank_id`=(SELECT `ls_rank_id` FROM `ffxiv__linkshell_rank` WHERE `rank`=:rank AND `rank` IS NOT NULL LIMIT 1), `current`=1;',
                        [
                            ':ls_id' => $this->id,
                            ':member_id' => $member,
                            ':rank' => (empty($details['ls_rank']) ? 'Member' : $details['ls_rank'])
                        ],
                    ];
                }
            }
            #Running the queries we've accumulated
            Query::query($queries);
            #Schedule a proper update of any newly added characters
            if (!empty($this->lodestone['members'])) {
                $this->charMassCron($this->lodestone['members']);
            }
            return true;
        } catch (\Throwable $exception) {
            Errors::error_log($exception, 'ls_id: '.$this->id);
            return false;
        }
    }
    
    /**
     * Delete linkshell
     * @return bool
     */
    protected function delete(): bool
    {
        try {
            $queries = [];
            #Remove characters from the group
            $queries[] = [
                'UPDATE `ffxiv__linkshell_character` SET `current`=0 WHERE `ls_id`=:group_id;',
                [':group_id' => $this->id,]
            ];
            #Update linkshell
            $queries[] = [
                'UPDATE `ffxiv__linkshell` SET `deleted` = COALESCE(`deleted`, UTC_DATE()), `updated`=CURRENT_TIMESTAMP() WHERE `ls_id` = :id',
                [':id' => $this->id],
            ];
            return Query::query($queries);
        } catch (\Throwable $exception) {
            Errors::error_log($exception, debug: $this->debug);
            return false;
        }
    }
}