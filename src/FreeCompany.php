<?php
declare(strict_types = 1);

namespace Simbiat\FFXIV;

use Simbiat\Cron\TaskInstance;
use Simbiat\Database\Query;
use Simbiat\Website\Errors;
use Simbiat\Website\Sanitization;
use function count;

/**
 * Class representing a FFXIV free company (guild)
 */
class FreeCompany extends AbstractTrackerEntity
{
    #Custom properties
    protected const string entityType = 'freecompany';
    public array $dates = [];
    public ?string $tag = null;
    public array $crest = [];
    public int $rank = 0;
    public ?string $slogan = null;
    public bool $recruiting = false;
    public ?string $community = null;
    public ?string $grandCompany = null;
    public ?string $active = null;
    public array $location = [];
    public array $focus = [];
    public array $seeking = [];
    public array $oldNames = [];
    public array $ranking = [];
    public array $members = [];
    
    /**
     * Function to get initial data from DB
     * @throws \Exception
     */
    protected function getFromDB(): array
    {
        #Get general information
        $data = Query::query('SELECT * FROM `ffxiv__freecompany` LEFT JOIN `ffxiv__server` ON `ffxiv__freecompany`.`serverid`=`ffxiv__server`.`serverid` LEFT JOIN `ffxiv__grandcompany` ON `ffxiv__freecompany`.`grandcompanyid`=`ffxiv__grandcompany`.`gcId` LEFT JOIN `ffxiv__timeactive` ON `ffxiv__freecompany`.`activeid`=`ffxiv__timeactive`.`activeid` LEFT JOIN `ffxiv__estate` ON `ffxiv__freecompany`.`estateid`=`ffxiv__estate`.`estateid` LEFT JOIN `ffxiv__city` ON `ffxiv__estate`.`cityid`=`ffxiv__city`.`cityid` WHERE `freecompanyid`=:id', [':id' => $this->id], return: 'row');
        #Return empty if nothing was found
        if (empty($data)) {
            return [];
        }
        #Get old names
        $data['oldnames'] = Query::query('SELECT `name` FROM `ffxiv__freecompany_names` WHERE `freecompanyid`=:id AND `name`!=:name', [':id' => $this->id, ':name' => $data['name']], return: 'column');
        #Get members
        $data['members'] = Query::query('SELECT \'character\' AS `type`, `ffxiv__freecompany_character`.`characterid` AS `id`, `ffxiv__freecompany_rank`.`rankid`, `rankname` AS `rank`, `name`, `ffxiv__character`.`avatar` AS `icon`, `userid` FROM `ffxiv__freecompany_character`LEFT JOIN `uc__user_to_ff_character` ON `uc__user_to_ff_character`.`characterid`=`ffxiv__freecompany_character`.`characterid` LEFT JOIN `ffxiv__freecompany_rank` ON `ffxiv__freecompany_rank`.`rankid`=`ffxiv__freecompany_character`.`rankid` AND `ffxiv__freecompany_rank`.`freecompanyid`=`ffxiv__freecompany_character`.`freecompanyid` LEFT JOIN `ffxiv__character` ON `ffxiv__character`.`characterid`=`ffxiv__freecompany_character`.`characterid` LEFT JOIN (SELECT `rankid`, COUNT(*) AS `total` FROM `ffxiv__freecompany_character` WHERE `ffxiv__freecompany_character`.`freecompanyid`=:id GROUP BY `rankid`) `ranklist` ON `ranklist`.`rankid` = `ffxiv__freecompany_character`.`rankid` WHERE `ffxiv__freecompany_character`.`freecompanyid`=:id AND `current`=1 ORDER BY `ranklist`.`total`, `ranklist`.`rankid` , `ffxiv__character`.`name`;', [':id' => $this->id], return: 'all');
        #History of ranks. Ensuring that we get only the freshest 100 entries sorted from latest to newest
        $data['ranks_history'] = Query::query('SELECT `date`, `weekly`, `monthly`, `members` FROM `ffxiv__freecompany_ranking` WHERE `freecompanyid`=:id ORDER BY `date` DESC LIMIT 100;', [':id' => $this->id], return: 'all');
        #Clean up the data from unnecessary (technical) clutter
        unset($data['gcId'], $data['estateid'], $data['gc_icon'], $data['activeid'], $data['cityid'], $data['left'], $data['top'], $data['cityicon']);
        #In case the entry is old enough (at least 1 day old) and register it for update. Also check that this is not a bot.
        if (empty($_SESSION['UA']['bot']) && (time() - strtotime($data['updated'])) >= 86400) {
            new TaskInstance()->settingsFromArray(['task' => 'ffUpdateEntity', 'arguments' => [(string)$this->id, 'freecompany'], 'message' => 'Updating free company with ID '.$this->id, 'priority' => 1])->add();
        }
        return $data;
    }
    
    /**
     * Get data from Lodestone
     * @param bool $allowSleep Whether to wait in case Lodestone throttles the request (that is throttle on our side)
     *
     * @return string|array
     */
    public function getFromLodestone(bool $allowSleep = false): string|array
    {
        $Lodestone = new Lodestone();
        $data = $Lodestone->getFreeCompany($this->id)->getFreeCompanyMembers($this->id, 0)->getResult();
        if (empty($data['freecompanies'][$this->id]['server']) || (empty($data['freecompanies'][$this->id]['members']) && (int)($data['freecompanies'][$this->id]['members_count'] ?? 0) > 0) || (!empty($data['freecompanies'][$this->id]['members']) && count($data['freecompanies'][$this->id]['members']) < (int)($data['freecompanies'][$this->id]['members_count'] ?? 0))) {
            if (!empty($data['freecompanies'][$this->id]) && (int)$data['freecompanies'][$this->id] === 404) {
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
                return 'Failed to get any data for Free Company '.$this->id;
            }
            return 'Failed to get all necessary data for Free Company '.$this->id.' ('.$Lodestone->getLastError()['url'].'): '.$Lodestone->getLastError()['error'];
        }
        if (empty($data['freecompanies'][$this->id]['crest'][2]) && !empty($data['freecompanies'][$this->id]['crest'][1])) {
            $data['freecompanies'][$this->id]['crest'][2] = $data['freecompanies'][$this->id]['crest'][1];
            $data['freecompanies'][$this->id]['crest'][1] = null;
        }
        $data = $data['freecompanies'][$this->id];
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
        $this->dates = [
            'formed' => strtotime($fromDB['formed']),
            'registered' => strtotime($fromDB['registered']),
            'updated' => strtotime($fromDB['updated']),
            'deleted' => (empty($fromDB['deleted']) ? null : strtotime($fromDB['deleted'])),
        ];
        $this->location = [
            'dataCenter' => $fromDB['datacenter'],
            'server' => $fromDB['server'],
            'estate' => [
                'region' => $fromDB['region'],
                'city' => $fromDB['city'],
                'area' => $fromDB['area'],
                'ward' => (int)$fromDB['ward'],
                'plot' => (int)$fromDB['plot'],
                'name' => $fromDB['estate_zone'],
                'size' => (int)$fromDB['size'],
                'message' => $fromDB['estate_message'],
            ],
        ];
        $this->tag = $fromDB['tag'];
        $this->crest = [
            0 => $fromDB['crest_part_1'],
            1 => $fromDB['crest_part_2'],
            2 => $fromDB['crest_part_3'],
        ];
        $this->rank = (int)$fromDB['rank'];
        $this->slogan = $fromDB['slogan'];
        $this->recruiting = (bool)$fromDB['recruitment'];
        $this->community = $fromDB['communityid'];
        $this->grandCompany = $fromDB['gcName'];
        $this->active = $fromDB['active'];
        $this->focus = [
            'Role-playing' => (bool)$fromDB['Role-playing'],
            'Leveling' => (bool)$fromDB['Leveling'],
            'Casual' => (bool)$fromDB['Casual'],
            'Hardcore' => (bool)$fromDB['Hardcore'],
            'Dungeons' => (bool)$fromDB['Dungeons'],
            'Guildhests' => (bool)$fromDB['Guildhests'],
            'Trials' => (bool)$fromDB['Trials'],
            'Raids' => (bool)$fromDB['Raids'],
            'PvP' => (bool)$fromDB['PvP'],
        ];
        $this->seeking = [
            'Tank' => (bool)$fromDB['Tank'],
            'Healer' => (bool)$fromDB['Healer'],
            'DPS' => (bool)$fromDB['DPS'],
            'Crafter' => (bool)$fromDB['Crafter'],
            'Gatherer' => (bool)$fromDB['Gatherer'],
        ];
        $this->oldNames = $fromDB['oldnames'];
        $this->ranking = $fromDB['ranks_history'];
        #Adjust types for ranking
        foreach ($this->ranking as $key => $rank) {
            $this->ranking[$key]['date'] = strtotime($rank['date']);
            $this->ranking[$key]['weekly'] = (int)$rank['weekly'];
            $this->ranking[$key]['monthly'] = (int)$rank['monthly'];
            $this->ranking[$key]['members'] = (int)$rank['members'];
        }
        $this->members = $fromDB['members'];
    }
    
    /**
     * Function to update the free company
     *
     * @return bool
     */
    protected function updateDB(): bool
    {
        try {
            #Download crest components
            $this->downloadCrestComponents($this->lodestone['crest']);
            if ($this->lodestone['active'] === 'Not specified') {
                $this->lodestone['active'] = null;
            }
            #Main query to insert or update a Free Company
            $queries[] = [
                'INSERT INTO `ffxiv__freecompany` (
                    `freecompanyid`, `name`, `serverid`, `formed`, `registered`, `updated`, `deleted`, `grandcompanyid`, `tag`, `crest_part_1`, `crest_part_2`, `crest_part_3`, `rank`, `slogan`, `activeid`, `recruitment`, `communityid`, `estate_zone`, `estateid`, `estate_message`, `Role-playing`, `Leveling`, `Casual`, `Hardcore`, `Dungeons`, `Guildhests`, `Trials`, `Raids`, `PvP`, `Tank`, `Healer`, `DPS`, `Crafter`, `Gatherer`
                )
                VALUES (
                    :freecompanyid, :name, (SELECT `serverid` FROM `ffxiv__server` WHERE `server`=:server), :formed, UTC_DATE(), CURRENT_TIMESTAMP(), NULL, (SELECT `gcId` FROM `ffxiv__grandcompany` WHERE `gcName`=:grandCompany), :tag, :crest_part_1, :crest_part_2, :crest_part_3, :rank, :slogan, (SELECT `activeid` FROM `ffxiv__timeactive` WHERE `active`=:active LIMIT 1), :recruitment, :communityid, :estate_zone, (SELECT `estateid` FROM `ffxiv__estate` WHERE CONCAT(\'Plot \', `plot`, \', \', `ward`, \' Ward, \', `area`, \' (\', CASE WHEN `size` = 1 THEN \'Small\' WHEN `size` = 2 THEN \'Medium\' WHEN `size` = 3 THEN \'Large\' END, \')\')=:estate_address LIMIT 1), :estate_message, :rolePlaying, :leveling, :casual, :hardcore, :dungeons, :guildhests, :trials, :raids, :pvp, :tank, :healer, :dps, :crafter, :gatherer
                )
                ON DUPLICATE KEY UPDATE
                    `name`=:name, `serverid`=(SELECT `serverid` FROM `ffxiv__server` WHERE `server`=:server), `formed`=:formed, `updated`=CURRENT_TIMESTAMP(), `deleted`=NULL, `grandcompanyid`=(SELECT `gcId` FROM `ffxiv__grandcompany` WHERE `gcName`=:grandCompany), `tag`=:tag, `crest_part_1`=:crest_part_1, `crest_part_2`=:crest_part_2, `crest_part_3`=:crest_part_3, `rank`=:rank, `slogan`=:slogan, `activeid`=(SELECT `activeid` FROM `ffxiv__timeactive` WHERE `active`=:active AND `active` IS NOT NULL LIMIT 1), `recruitment`=:recruitment, `communityid`=:communityid, `estate_zone`=:estate_zone, `estateid`=(SELECT `estateid` FROM `ffxiv__estate` WHERE CONCAT(\'Plot \', `plot`, \', \', `ward`, \' Ward, \', `area`, \' (\', CASE WHEN `size` = 1 THEN \'Small\' WHEN `size` = 2 THEN \'Medium\' WHEN `size` = 3 THEN \'Large\' END, \')\')=:estate_address LIMIT 1), `estate_message`=:estate_message, `Role-playing`=:rolePlaying, `Leveling`=:leveling, `Casual`=:casual, `Hardcore`=:hardcore, `Dungeons`=:dungeons, `Guildhests`=:guildhests, `Trials`=:trials, `Raids`=:raids, `PvP`=:pvp, `Tank`=:tank, `Healer`=:healer, `DPS`=:dps, `Crafter`=:crafter, `Gatherer`=:gatherer;',
                [
                    ':freecompanyid' => $this->id,
                    ':name' => $this->lodestone['name'],
                    ':server' => $this->lodestone['server'],
                    ':formed' => [$this->lodestone['formed'], 'date'],
                    ':grandCompany' => $this->lodestone['grandCompany'],
                    ':tag' => $this->lodestone['tag'],
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
                    ':rank' => $this->lodestone['rank'],
                    ':slogan' => [
                        (empty($this->lodestone['slogan']) ? NULL : Sanitization::sanitizeHTML($this->lodestone['slogan'])),
                        (empty($this->lodestone['slogan']) ? 'null' : 'string'),
                    ],
                    ':active' => [
                        (empty($this->lodestone['active']) ? NULL : $this->lodestone['active']),
                        (empty($this->lodestone['active']) ? 'null' : 'string'),
                    ],
                    ':recruitment' => (strcasecmp($this->lodestone['recruitment'], 'Open') === 0 ? 1 : 0),
                    ':estate_zone' => [
                        (empty($this->lodestone['estate']['name']) ? NULL : $this->lodestone['estate']['name']),
                        (empty($this->lodestone['estate']['name']) ? 'null' : 'string'),
                    ],
                    ':estate_address' => [
                        (empty($this->lodestone['estate']['address']) ? NULL : $this->lodestone['estate']['address']),
                        (empty($this->lodestone['estate']['address']) ? 'null' : 'string'),
                    ],
                    ':estate_message' => [
                        (empty($this->lodestone['estate']['greeting']) ? NULL : Sanitization::sanitizeHTML($this->lodestone['estate']['greeting'])),
                        (empty($this->lodestone['estate']['greeting']) ? 'null' : 'string'),
                    ],
                    ':rolePlaying' => (empty($this->lodestone['focus']) ? 0 : $this->lodestone['focus'][array_search('Role-playing', array_column($this->lodestone['focus'], 'name'), true)]['enabled']),
                    ':leveling' => (empty($this->lodestone['focus']) ? 0 : $this->lodestone['focus'][array_search('Leveling', array_column($this->lodestone['focus'], 'name'), true)]['enabled']),
                    ':casual' => (empty($this->lodestone['focus']) ? 0 : $this->lodestone['focus'][array_search('Casual', array_column($this->lodestone['focus'], 'name'), true)]['enabled']),
                    ':hardcore' => (empty($this->lodestone['focus']) ? 0 : $this->lodestone['focus'][array_search('Hardcore', array_column($this->lodestone['focus'], 'name'), true)]['enabled']),
                    ':dungeons' => (empty($this->lodestone['focus']) ? 0 : $this->lodestone['focus'][array_search('Dungeons', array_column($this->lodestone['focus'], 'name'), true)]['enabled']),
                    ':guildhests' => (empty($this->lodestone['focus']) ? 0 : $this->lodestone['focus'][array_search('Guildhests', array_column($this->lodestone['focus'], 'name'), true)]['enabled']),
                    ':trials' => (empty($this->lodestone['focus']) ? 0 : $this->lodestone['focus'][array_search('Trials', array_column($this->lodestone['focus'], 'name'), true)]['enabled']),
                    ':raids' => (empty($this->lodestone['focus']) ? 0 : $this->lodestone['focus'][array_search('Raids', array_column($this->lodestone['focus'], 'name'), true)]['enabled']),
                    ':pvp' => (empty($this->lodestone['focus']) ? 0 : $this->lodestone['focus'][array_search('PvP', array_column($this->lodestone['focus'], 'name'), true)]['enabled']),
                    ':tank' => (empty($this->lodestone['seeking']) ? 0 : $this->lodestone['seeking'][array_search('Tank', array_column($this->lodestone['seeking'], 'name'), true)]['enabled']),
                    ':healer' => (empty($this->lodestone['seeking']) ? 0 : $this->lodestone['seeking'][array_search('Healer', array_column($this->lodestone['seeking'], 'name'), true)]['enabled']),
                    ':dps' => (empty($this->lodestone['seeking']) ? 0 : $this->lodestone['seeking'][array_search('DPS', array_column($this->lodestone['seeking'], 'name'), true)]['enabled']),
                    ':crafter' => (empty($this->lodestone['seeking']) ? 0 : $this->lodestone['seeking'][array_search('Crafter', array_column($this->lodestone['seeking'], 'name'), true)]['enabled']),
                    ':gatherer' => (empty($this->lodestone['seeking']) ? 0 : $this->lodestone['seeking'][array_search('Gatherer', array_column($this->lodestone['seeking'], 'name'), true)]['enabled']),
                    ':communityid' => [
                        (empty($this->lodestone['communityid']) ? NULL : $this->lodestone['communityid']),
                        (empty($this->lodestone['communityid']) ? 'null' : 'string'),
                    ],
                ],
            ];
            #Register the Free Company name if it's not registered already
            $queries[] = [
                'INSERT IGNORE INTO `ffxiv__freecompany_names`(`freecompanyid`, `name`) VALUES (:freecompanyid, :name);',
                [
                    ':freecompanyid' => $this->id,
                    ':name' => $this->lodestone['name'],
                ],
            ];
            #Adding ranking
            if (!empty($this->lodestone['members']) && !empty($this->lodestone['weekly_rank']) && !empty($this->lodestone['monthly_rank'])) {
                $queries[] = [
                    'INSERT IGNORE INTO `ffxiv__freecompany_ranking` (`freecompanyid`, `date`, `weekly`, `monthly`, `members`) SELECT * FROM (SELECT :freecompanyid AS `freecompanyid`, UTC_DATE() AS `date`, :weekly AS `weekly`, :monthly AS `monthly`, :members AS `members` FROM DUAL WHERE :freecompanyid NOT IN (SELECT `freecompanyid` FROM (SELECT * FROM `ffxiv__freecompany_ranking` WHERE `freecompanyid`=:freecompanyid ORDER BY `date` DESC LIMIT 1) `lastrecord` WHERE `weekly`=:weekly AND `monthly`=:monthly) LIMIT 1) `actualinsert`;',
                    [
                        ':freecompanyid' => $this->id,
                        ':weekly' => [$this->lodestone['weekly_rank'], 'int'],
                        ':monthly' => [$this->lodestone['monthly_rank'], 'int'],
                        ':members' => [count($this->lodestone['members']), 'int'],
                    ],
                ];
            }
            #Get members as registered on the tracker
            $trackMembers = Query::query('SELECT `characterid` FROM `ffxiv__freecompany_character` WHERE `freecompanyid`=:fcId AND `current`=1;', [':fcId' => $this->id], return: 'column');
            #Process members that left the company
            foreach ($trackMembers as $member) {
                #Check if member from tracker is present in a Lodestone list
                if (!isset($this->lodestone['members'][$member])) {
                    #Update status for the character
                    $queries[] = [
                        'UPDATE `ffxiv__freecompany_character` SET `current`=0 WHERE `freecompanyid`=:fcId AND `characterid`=:characterid;',
                        [
                            ':characterid' => $member,
                            ':fcId' => $this->id,
                        ],
                    ];
                }
            }
            #Process Lodestone members
            if (!empty($this->lodestone['members'])) {
                foreach ($this->lodestone['members'] as $member => $details) {
                    #Register or update rank name
                    $queries[] = [
                        'INSERT INTO `ffxiv__freecompany_rank` (`freecompanyid`, `rankid`, `rankname`) VALUE (:freecompanyid, :rankid, :rankName) ON DUPLICATE KEY UPDATE `rankname`=:rankName',
                        [
                            ':freecompanyid' => $this->id,
                            ':rankid' => $details['rankid'],
                            ':rankName' => (empty($details['rank']) ? '' : $details['rank']),
                        ],
                    ];
                    #Check if a member is registered on the tracker while saving the status for future use
                    $this->lodestone['members'][$member]['registered'] = Query::query('SELECT `characterid` FROM `ffxiv__character` WHERE `characterid`=:characterid', [':characterid' => $member], return: 'check');
                    if (!$this->lodestone['members'][$member]['registered']) {
                        #Create the basic entry of the character
                        $queries[] = [
                            'INSERT INTO `ffxiv__character`(
                                `characterid`, `serverid`, `name`, `registered`, `updated`, `avatar`, `gcrankid`
                            )
                            VALUES (
                                :characterid, (SELECT `serverid` FROM `ffxiv__server` WHERE `server`=:server), :name, UTC_DATE(), TIMESTAMPADD(SECOND, -3600, CURRENT_TIMESTAMP()), :avatar, `gcrankid` = (SELECT `gcrankid` FROM `ffxiv__grandcompany_rank` WHERE `gc_rank`=:gcRank ORDER BY `gcrankid` LIMIT 1)
                            ) ON DUPLICATE KEY UPDATE `deleted`=NULL;',
                            [
                                ':characterid' => $member,
                                ':server' => $details['server'],
                                ':name' => $details['name'],
                                ':avatar' => str_replace(['https://img2.finalfantasyxiv.com/f/', 'c0.jpg'], '', $details['avatar']),
                                ':gcRank' => (empty($details['grandCompany']['rank']) ? '' : $details['grandCompany']['rank']),
                            ]
                        ];
                    }
                    #Link the character to the company
                    $queries[] = [
                        'INSERT INTO `ffxiv__freecompany_character` (`freecompanyid`, `characterid`, `rankid`, `current`) VALUES (:fcId, :characterid, :rankid, 1) ON DUPLICATE KEY UPDATE `current`=1, `rankid`=:rankid;',
                        [
                            ':characterid' => $member,
                            ':fcId' => $this->id,
                            ':rankid' => $details['rankid'],
                        ],
                    ];
                }
            }
            #Running the queries we've accumulated
            Query::query($queries);
            #Schedule the proper update of any newly added characters
            if (!empty($this->lodestone['members'])) {
                $this->charMassCron($this->lodestone['members']);
            }
            return true;
        } catch (\Throwable $e) {
            Errors::error_log($e, 'freecompanyid: '.$this->id);
            return false;
        }
    }
    
    /** Delete free company
     * @return bool
     */
    protected function delete(): bool
    {
        try {
            $queries = [];
            #Remove characters from thegroup
            $queries[] = [
                'UPDATE `ffxiv__freecompany_character` SET `current`=0 WHERE `freecompanyid`=:groupId;',
                [':groupId' => $this->id,]
            ];
            #Update Free Company
            $queries[] = [
                'UPDATE `ffxiv__freecompany` SET `deleted` = COALESCE(`deleted`, UTC_DATE()), `updated`=CURRENT_TIMESTAMP() WHERE `freecompanyid` = :id',
                [':id' => $this->id],
            ];
            return Query::query($queries);
        } catch (\Throwable $e) {
            Errors::error_log($e, debug: $this->debug);
            return false;
        }
    }
}