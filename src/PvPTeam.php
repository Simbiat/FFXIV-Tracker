<?php
declare(strict_types = 1);

namespace Simbiat\FFXIV;

use Simbiat\Cron\TaskInstance;
use Simbiat\Database\Query;
use Simbiat\Website\Errors;

/**
 * Class representing a FFXIV PvP Team
 */
class PvPTeam extends AbstractTrackerEntity
{
    #Custom properties
    protected const string entityType = 'pvpteam';
    protected string $idFormat = '/^[a-z\d]{40}$/m';
    public array $dates = [];
    public ?string $community = null;
    public array $crest = [];
    public string $dataCenter;
    public array $oldNames = [];
    public array $members = [];
    
    /**
     * Function to get initial data from DB
     * @throws \Exception
     */
    protected function getFromDB(): array
    {
        #Get general information
        $data = Query::query('SELECT * FROM `ffxiv__pvpteam` LEFT JOIN `ffxiv__server` ON `ffxiv__pvpteam`.`datacenterid`=`ffxiv__server`.`serverid` WHERE `pvpteamid`=:id', [':id' => $this->id], return: 'row');
        #Return empty if nothing was found
        if (empty($data)) {
            return [];
        }
        #Get old names
        $data['oldnames'] = Query::query('SELECT `name` FROM `ffxiv__pvpteam_names` WHERE `pvpteamid`=:id AND `name`<>:name', [':id' => $this->id, ':name' => $data['name']], return: 'column');
        #Get members
        $data['members'] = Query::query('SELECT \'character\' AS `type`, `ffxiv__pvpteam_character`.`characterid` AS `id`, `ffxiv__character`.`pvp_matches` AS `matches`, `ffxiv__character`.`name`, `ffxiv__character`.`avatar` AS `icon`, `ffxiv__pvpteam_rank`.`rank`, `ffxiv__pvpteam_rank`.`pvprankid`, `userid` FROM `ffxiv__pvpteam_character` LEFT JOIN `uc__user_to_ff_character` ON `uc__user_to_ff_character`.`characterid`=`ffxiv__pvpteam_character`.`characterid` LEFT JOIN `ffxiv__pvpteam_rank` ON `ffxiv__pvpteam_rank`.`pvprankid`=`ffxiv__pvpteam_character`.`rankid` LEFT JOIN `ffxiv__character` ON `ffxiv__pvpteam_character`.`characterid`=`ffxiv__character`.`characterid` WHERE `ffxiv__pvpteam_character`.`pvpteamid`=:id AND `current`=1 ORDER BY `ffxiv__pvpteam_character`.`rankid` , `ffxiv__character`.`name` ', [':id' => $this->id], return: 'all');
        #Clean up the data from unnecessary (technical) clutter
        unset($data['datacenterid'], $data['serverid'], $data['server']);
        #In case the entry is old enough (at least 1 day old) and register it for update. Also check that this is not a bot.
        if (empty($_SESSION['UA']['bot']) && (time() - strtotime($data['updated'])) >= 86400) {
            new TaskInstance()->settingsFromArray(['task' => 'ffUpdateEntity', 'arguments' => [(string)$this->id, 'pvpteam'], 'message' => 'Updating PvP team with ID '.$this->id, 'priority' => 1])->add();
        }
        return $data;
    }
    
    /**
     * Get PvP team data from Lodestone
     * @param bool $allowSleep Whether to wait in case Lodestone throttles the request (that is throttle on our side)
     *
     * @return string|array
     */
    public function getFromLodestone(bool $allowSleep = false): string|array
    {
        $Lodestone = new Lodestone();
        $data = $Lodestone->getPvPTeam($this->id)->getResult();
        if (empty($data['pvpteams'][$this->id]['dataCenter']) || empty($data['pvpteams'][$this->id]['members'])) {
            if (!empty($data['pvpteams'][$this->id]['members']) && (int)$data['pvpteams'][$this->id]['members'] === 404) {
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
                return 'Failed to get any data for PvP Team '.$this->id;
            }
            return 'Failed to get all necessary data for PvP Team '.$this->id.' ('.$Lodestone->getLastError()['url'].'): '.$Lodestone->getLastError()['error'];
        }
        if (empty($data['pvpteams'][$this->id]['crest'][2]) && !empty($data['pvpteams'][$this->id]['crest'][1])) {
            $data['pvpteams'][$this->id]['crest'][2] = $data['pvpteams'][$this->id]['crest'][1];
            $data['pvpteams'][$this->id]['crest'][1] = null;
        }
        $data = $data['pvpteams'][$this->id];
        $data['id'] = $this->id;
        $data['404'] = false;
        unset($data['pageCurrent'], $data['pageTotal']);
        return $data;
    }
    
    /**
     * Process data from DB
     * @param array $fromDB
     *
     * @return void
     */
    protected function process(array $fromDB): void
    {
        $this->name = $fromDB['name'];
        $this->dates = [
            'formed' => (empty($fromDB['formed']) ? null : strtotime($fromDB['formed'])),
            'registered' => strtotime($fromDB['registered']),
            'updated' => strtotime($fromDB['updated']),
            'deleted' => (empty($fromDB['deleted']) ? null : strtotime($fromDB['deleted'])),
        ];
        $this->community = $fromDB['communityid'];
        $this->crest = [
            0 => $fromDB['crest_part_1'],
            1 => $fromDB['crest_part_2'],
            2 => $fromDB['crest_part_3'],
        ];
        $this->dataCenter = $fromDB['datacenter'];
        $this->oldNames = $fromDB['oldnames'];
        $this->members = $fromDB['members'];
        foreach ($this->members as $key => $member) {
            $this->members[$key]['matches'] = (int)$member['matches'];
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
            #Download crest components
            $this->downloadCrestComponents($this->lodestone['crest']);
            #Main query to insert or update a PvP Team
            $queries[] = [
                'INSERT INTO `ffxiv__pvpteam` (`pvpteamid`, `name`, `formed`, `registered`, `updated`, `deleted`, `datacenterid`, `communityid`, `crest_part_1`, `crest_part_2`, `crest_part_3`) VALUES (:pvpteamid, :name, :formed, UTC_DATE(), CURRENT_TIMESTAMP(), NULL, (SELECT `serverid` FROM `ffxiv__server` WHERE `datacenter`=:datacenter ORDER BY `serverid` LIMIT 1), :communityid, :crest_part_1, :crest_part_2, :crest_part_3) ON DUPLICATE KEY UPDATE `name`=:name, `formed`=:formed, `updated`=CURRENT_TIMESTAMP(), `deleted`=NULL, `datacenterid`=(SELECT `serverid` FROM `ffxiv__server` WHERE `datacenter`=:datacenter ORDER BY `serverid` LIMIT 1), `communityid`=:communityid, `crest_part_1`=:crest_part_1, `crest_part_2`=:crest_part_2, `crest_part_3`=:crest_part_3;',
                [
                    ':pvpteamid' => $this->id,
                    ':datacenter' => $this->lodestone['dataCenter'],
                    ':name' => $this->lodestone['name'],
                    ':formed' => [$this->lodestone['formed'], 'date'],
                    ':communityid' => [
                        (empty($this->lodestone['communityid']) ? NULL : $this->lodestone['communityid']),
                        (empty($this->lodestone['communityid']) ? 'null' : 'string'),
                    ],
                    ':crest_part_1' => [
                        (empty($this->lodestone['crest'][0]) ? NULL : $this->lodestone['crest'][0]),
                        (empty($this->lodestone['crest'][0]) ? 'null' : 'string'),
                    ],
                    ':crest_part_2' => [
                        (empty($this->lodestone['crest'][1]) ? NULL : $this->lodestone['crest'][1]),
                        (empty($this->lodestone['crest'][1]) ? 'null' : 'string'),
                    ],
                    ':crest_part_3' => [
                        (empty($this->lodestone['crest'][2]) ? NULL : $this->lodestone['crest'][2]),
                        (empty($this->lodestone['crest'][2]) ? 'null' : 'string'),
                    ],
                ],
            ];
            #Register PvP Team name if it's not registered already
            $queries[] = [
                'INSERT IGNORE INTO `ffxiv__pvpteam_names`(`pvpteamid`, `name`) VALUES (:pvpteamid, :name);',
                [
                    ':pvpteamid' => $this->id,
                    ':name' => $this->lodestone['name'],
                ],
            ];
            #Get members as registered on the tracker
            $trackMembers = Query::query('SELECT `characterid` FROM `ffxiv__pvpteam_character` WHERE `pvpteamid`=:pvpteamid AND `current`=1;', [':pvpteamid' => $this->id], return: 'column');
            #Process members that left the team
            foreach ($trackMembers as $member) {
                #Check if member from tracker is present in a Lodestone list
                if (!isset($this->lodestone['members'][$member])) {
                    #Update status for the character
                    $queries[] = [
                        'UPDATE `ffxiv__pvpteam_character` SET `current`=0 WHERE `pvpteamid`=:pvpId AND `characterid`=:characterid;',
                        [
                            ':characterid' => $member,
                            ':pvpId' => $this->id,
                        ],
                    ];
                }
            }
            #Process Lodestone members
            if (!empty($this->lodestone['members'])) {
                foreach ($this->lodestone['members'] as $member => $details) {
                    #Check if a member is registered on the tracker while saving the status for future use
                    $this->lodestone['members'][$member]['registered'] = Query::query('SELECT `characterid` FROM `ffxiv__character` WHERE `characterid`=:characterid', [':characterid' => $member], return: 'check');
                    if (!$this->lodestone['members'][$member]['registered']) {
                        #Create a basic entry of the character
                        $queries[] = [
                            'INSERT IGNORE INTO `ffxiv__character`(
                                `characterid`, `serverid`, `name`, `registered`, `updated`, `avatar`, `gcrankid`, `pvp_matches`
                            )
                            VALUES (
                                :characterid, (SELECT `serverid` FROM `ffxiv__server` WHERE `server`=:server), :name, UTC_DATE(), TIMESTAMPADD(SECOND, -3600, CURRENT_TIMESTAMP()), :avatar, `gcrankid` = (SELECT `gcrankid` FROM `ffxiv__grandcompany_rank` WHERE `gc_rank`=:gcRank ORDER BY `gcrankid` LIMIT 1), :matches
                            ) ON DUPLICATE KEY UPDATE `deleted`=NULL;',
                            [
                                ':characterid' => $member,
                                ':server' => $details['server'],
                                ':name' => $details['name'],
                                ':avatar' => str_replace(['https://img2.finalfantasyxiv.com/f/', 'c0.jpg'], '', $details['avatar']),
                                ':gcRank' => (empty($details['grandCompany']['rank']) ? '' : $details['grandCompany']['rank']),
                                ':matches' => (empty($details['feasts']) ? 0 : $details['feasts']),
                            ]
                        ];
                    }
                    #Link the character to the team
                    $queries[] = [
                        'INSERT INTO `ffxiv__pvpteam_character` (`pvpteamid`, `characterid`, `rankid`, `current`) VALUES (:pvpteamId, :characterid, (SELECT `pvprankid` FROM `ffxiv__pvpteam_rank` WHERE `rank`=:rank LIMIT 1), 1) ON DUPLICATE KEY UPDATE `current`=1, `rankid`=(SELECT `pvprankid` FROM `ffxiv__pvpteam_rank` WHERE `rank`=:rank AND `rank` IS NOT NULL LIMIT 1);',
                        [
                            ':characterid' => $member,
                            ':pvpteamId' => $this->id,
                            ':rank' => $details['rank'] ?? 'Member',
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
        } catch (\Throwable $e) {
            Errors::error_log($e, 'pvpteamid: '.$this->id);
            return false;
        }
    }
    
    /**
     * Delete PvP Team
     * @return bool
     */
    protected function delete(): bool
    {
        try {
            $queries = [];
            #Remove characters from the group
            $queries[] = [
                'UPDATE `ffxiv__pvpteam_character` SET `current`=0 WHERE `pvpteamid`=:groupId;',
                [':groupId' => $this->id,]
            ];
            #Update PvP Team
            $queries[] = [
                'UPDATE `ffxiv__pvpteam` SET `deleted` = COALESCE(`deleted`, UTC_DATE()), `updated`=CURRENT_TIMESTAMP() WHERE `pvpteamid` = :id', [':id' => $this->id],
            ];
            return Query::query($queries);
        } catch (\Throwable $e) {
            Errors::error_log($e, debug: $this->debug);
            return false;
        }
    }
}