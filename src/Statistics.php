<?php
declare(strict_types = 1);

namespace Simbiat\FFXIV;

use JetBrains\PhpStorm\ExpectedValues;
use Simbiat\Arrays\Converters;
use Simbiat\Arrays\Editors;
use Simbiat\Arrays\Splitters;
use Simbiat\Cron\TaskInstance;
use Simbiat\Database\Select;
use Simbiat\Website\Config;
use function in_array;

/**
 * Class responsible for generating FFXIV statistics
 */
class Statistics
{
    /**
     * List of supported statistics types
     */
    private const array statisticsType = ['raw', 'characters', 'groups', 'achievements', 'timelines', 'bugs', 'other'];
    
    /**
     * @param string $type
     *
     * @return array
     * @throws \JsonException
     * @throws \Exception
     */
    public function update(#[ExpectedValues(self::statisticsType)] string $type = 'other'): array
    {
        $data = [];
        #Sanitize type
        if (!in_array($type, self::statisticsType, true)) {
            $type = 'other';
        }
        #Create a path if missing
        if (!is_dir(Config::$statistics) && !mkdir(Config::$statistics) && !is_dir(Config::$statistics)) {
            throw new \RuntimeException(\sprintf('Directory "%s" was not created', Config::$statistics));
        }
        $cachePath = Config::$statistics.$type.'.json';
        $data['time'] = time();
        switch ($type) {
            case 'raw':
                $this->getRaw($data);
                break;
            case 'characters':
                $this->getCharacters($data);
                break;
            case 'groups':
                $this->getGroups($data);
                break;
            case 'achievements':
                $this->getAchievements($data);
                break;
            case 'timelines':
                $this->getTimelines($data);
                break;
            case 'bugs':
                $this->getBugs($data);
                break;
            case 'other':
                $this->getOthers($data);
                break;
        }
        #Attempt to write to cache
        file_put_contents($cachePath, json_encode($data, JSON_INVALID_UTF8_SUBSTITUTE | JSON_OBJECT_AS_ARRAY | JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION | JSON_PRETTY_PRINT));
        if ($type === 'bugs') {
            $this->scheduleBugs($data);
        }
        return $data;
    }
    
    /**
     * Get data raw data for character statistics based on counts
     *
     * @param array $data Array of gathered data
     *
     * @return void
     */
    private function getRaw(array &$data): void
    {
        $data['raw'] = Select::selectAll(
            'SELECT COUNT(*) as `count`, `ffxiv__clan`.`race`, `ffxiv__clan`.`clan`, `ffxiv__character`.`genderid`, `ffxiv__guardian`.`guardian`, `ffxiv__city`.`city`, `ffxiv__grandcompany`.`gcName` FROM `ffxiv__character`
                                LEFT JOIN `ffxiv__clan` ON `ffxiv__character`.`clanid`=`ffxiv__clan`.`clanid`
                                LEFT JOIN `ffxiv__city` ON `ffxiv__character`.`cityid`=`ffxiv__city`.`cityid`
                                LEFT JOIN `ffxiv__guardian` ON `ffxiv__character`.`guardianid`=`ffxiv__guardian`.`guardianid`
                                LEFT JOIN `ffxiv__grandcompany_rank` ON `ffxiv__character`.`gcrankid`=`ffxiv__grandcompany_rank`.`gcrankid`
                                LEFT JOIN `ffxiv__grandcompany` ON `ffxiv__grandcompany_rank`.`gcId`=`ffxiv__grandcompany`.`gcId`
                                WHERE `ffxiv__character`.`clanid` IS NOT NULL GROUP BY `ffxiv__clan`.`race`, `ffxiv__clan`.`clan`, `ffxiv__character`.`genderid`, `ffxiv__guardian`.`guardian`, `ffxiv__city`.`cityid`, `ffxiv__grandcompany_rank`.`gcId` ORDER BY `count` DESC;
                    ');
    }
    
    /**
     * Get data for character-based statistics
     *
     * @param array $data Array of gathered data
     *
     * @return void
     */
    private function getCharacters(array &$data): void
    {
        #Jobs popularity
        $data['characters']['jobs'] = Select::selectPair(
            'SELECT `name`, SUM(`level`) as `sum` FROM `ffxiv__character_jobs` LEFT JOIN `ffxiv__jobs` ON `ffxiv__jobs`.`jobid`=`ffxiv__character_jobs`.`jobid` GROUP BY `ffxiv__character_jobs`.`jobid` ORDER BY `sum` DESC;'
        );
        #Most name changes
        $data['characters']['changes']['name'] = Select::selectAll('SELECT `tempresult`.`characterid` AS `id`, `ffxiv__character`.`avatar` AS `icon`, \'character\' AS `type`, `ffxiv__character`.`name` AS `value`, `count` FROM (SELECT `ffxiv__character_names`.`characterid`, count(`ffxiv__character_names`.`characterid`) AS `count` FROM `ffxiv__character_names` GROUP BY `ffxiv__character_names`.`characterid` ORDER BY `count` DESC LIMIT 20) `tempresult` INNER JOIN `ffxiv__character` ON `tempresult`.`characterid`=`ffxiv__character`.`characterid` ORDER BY `count` DESC');
        Editors::renameColumn($data['characters']['changes']['name'], 'value', 'name');
        #Most reincarnation
        $data['characters']['changes']['clan'] = Select::selectAll('SELECT `tempresult`.`characterid` AS `id`, `ffxiv__character`.`avatar` AS `icon`, \'character\' AS `type`, `ffxiv__character`.`name` AS `value`, `count` FROM (SELECT `ffxiv__character_clans`.`characterid`, count(`ffxiv__character_clans`.`characterid`) AS `count` FROM `ffxiv__character_clans` GROUP BY `ffxiv__character_clans`.`characterid` ORDER BY `count` DESC LIMIT 20) `tempresult` INNER JOIN `ffxiv__character` ON `tempresult`.`characterid`=`ffxiv__character`.`characterid` ORDER BY `count` DESC');
        Editors::renameColumn($data['characters']['changes']['clan'], 'value', 'name');
        #Most servers
        $data['characters']['changes']['server'] = Select::selectAll('SELECT `tempresult`.`characterid` AS `id`, `ffxiv__character`.`avatar` AS `icon`, \'character\' AS `type`, `ffxiv__character`.`name` AS `value`, `count` FROM (SELECT `ffxiv__character_servers`.`characterid`, count(`ffxiv__character_servers`.`characterid`) AS `count` FROM `ffxiv__character_servers` GROUP BY `ffxiv__character_servers`.`characterid` ORDER BY `count` DESC LIMIT 20) `tempresult` INNER JOIN `ffxiv__character` ON `tempresult`.`characterid`=`ffxiv__character`.`characterid` ORDER BY `count` DESC');
        Editors::renameColumn($data['characters']['changes']['server'], 'value', 'name');
        #Most companies
        $data['characters']['groups']['Free Companies'] = Select::selectAll('SELECT `tempresult`.`characterid` AS `id`, `ffxiv__character`.`avatar` AS `icon`, \'character\' AS `type`, `ffxiv__character`.`name` AS `value`, `count` FROM (SELECT `ffxiv__freecompany_character`.`characterid`, count(`ffxiv__freecompany_character`.`characterid`) AS `count` FROM `ffxiv__freecompany_character` GROUP BY `ffxiv__freecompany_character`.`characterid` ORDER BY `count` DESC LIMIT 20) `tempresult` INNER JOIN `ffxiv__character` ON `tempresult`.`characterid`=`ffxiv__character`.`characterid` ORDER BY `count` DESC');
        Editors::renameColumn($data['characters']['groups']['Free Companies'], 'value', 'name');
        #Most PvP teams
        $data['characters']['groups']['PvP Teams'] = Select::selectAll('SELECT `tempresult`.`characterid` AS `id`, `ffxiv__character`.`avatar` AS `icon`, \'character\' AS `type`, `ffxiv__character`.`name` AS `value`, `count` FROM (SELECT `ffxiv__pvpteam_character`.`characterid`, count(`ffxiv__pvpteam_character`.`characterid`) AS `count` FROM `ffxiv__pvpteam_character` GROUP BY `ffxiv__pvpteam_character`.`characterid` ORDER BY `count` DESC LIMIT 20) `tempresult` INNER JOIN `ffxiv__character` ON `tempresult`.`characterid`=`ffxiv__character`.`characterid` ORDER BY `count` DESC');
        Editors::renameColumn($data['characters']['groups']['PvP Teams'], 'value', 'name');
        #Most x-linkshells
        $data['characters']['groups']['Linkshells'] = Select::selectAll('SELECT `tempresult`.`characterid` AS `id`, `ffxiv__character`.`avatar` AS `icon`, \'character\' AS `type`, `ffxiv__character`.`name` AS `value`, `count` FROM (SELECT `ffxiv__linkshell_character`.`characterid`, count(`ffxiv__linkshell_character`.`characterid`) AS `count` FROM `ffxiv__linkshell_character` GROUP BY `ffxiv__linkshell_character`.`characterid` ORDER BY `count` DESC LIMIT 20) `tempresult` INNER JOIN `ffxiv__character` ON `tempresult`.`characterid`=`ffxiv__character`.`characterid` ORDER BY `count` DESC');
        Editors::renameColumn($data['characters']['groups']['Linkshells'], 'value', 'name');
        #Most linkshells
        $data['characters']['groups']['simLinkshells'] = Select::selectAll('SELECT `tempresult`.`characterid` AS `id`, `ffxiv__character`.`avatar` AS `icon`, \'character\' AS `type`, `ffxiv__character`.`name` AS `value`, `count` FROM (SELECT `ffxiv__linkshell_character`.`characterid`, count(`ffxiv__linkshell_character`.`characterid`) AS `count` FROM `ffxiv__linkshell_character` WHERE `ffxiv__linkshell_character`.`current`=1 GROUP BY `ffxiv__linkshell_character`.`characterid` ORDER BY `count` DESC LIMIT 20) `tempresult` INNER JOIN `ffxiv__character` ON `tempresult`.`characterid`=`ffxiv__character`.`characterid` ORDER BY `count` DESC');
        Editors::renameColumn($data['characters']['groups']['simLinkshells'], 'value', 'name');
        #Groups affiliation
        $data['characters']['groups']['participation'] = Select::selectAll('
                        SELECT COUNT(*) as `count`,
                            (CASE
                                WHEN (`fc`=1 AND `pvp`=0 AND `ls`=0) THEN \'Free Company only\'
                                WHEN (`fc`=0 AND `pvp`=1 AND `ls`=0) THEN \'PvP Team only\'
                                WHEN (`fc`=0 AND `pvp`=0 AND `ls`=1) THEN \'Linkshell only\'
                                WHEN (`fc`=1 AND `pvp`=1 AND `ls`=0) THEN \'Free Company and PvP Team\'
                                WHEN (`fc`=1 AND `pvp`=0 AND `ls`=1) THEN \'Free Company and Linkshell\'
                                WHEN (`fc`=0 AND `pvp`=1 AND `ls`=1) THEN \'PvP Team and Linkshell\'
                                WHEN (`fc`=1 AND `pvp`=1 AND `ls`=1) THEN \'Free Company, PvP Team and Linkshell\'
                                ELSE \'No groups\'
                            END) AS `affiliation`
                        FROM (
                            SELECT `characterid`,
                                EXISTS(SELECT `characterid` FROM `ffxiv__freecompany_character` WHERE `characterid`=`main`.`characterid` AND `current`=1) as `fc`,
                                EXISTS(SELECT `characterid` FROM `ffxiv__pvpteam_character` WHERE `characterid`=`main`.`characterid` AND `current`=1) as `pvp`,
                                EXISTS(SELECT `characterid` FROM `ffxiv__linkshell_character` WHERE `characterid`=`main`.`characterid` AND `current`=1) as `ls`
                            FROM `ffxiv__character` AS `main` WHERE `deleted` IS NULL
                        ) as `temp`
                        GROUP BY `affiliation`;
                    ');
        #Get characters with most PvP matches. Using regular SQL since we do not count unique values, but rather use the regular column values
        $data['characters']['most_pvp'] = Select::selectAll('SELECT `ffxiv__character`.`characterid` AS `id`, `ffxiv__character`.`avatar` AS `icon`, \'character\' AS `type`, `ffxiv__character`.`name`, `pvp_matches` AS `count` FROM `ffxiv__character` ORDER BY `ffxiv__character`.`pvp_matches` DESC LIMIT 20');
        #Characters
        $data['servers']['characters'] = Select::selectAll('SELECT `ffxiv__character`.`genderid`, `ffxiv__server`.`server` AS `value`, count(`ffxiv__character`.`serverid`) AS `count` FROM `ffxiv__character` INNER JOIN `ffxiv__server` ON `ffxiv__character`.`serverid`=`ffxiv__server`.`serverid` WHERE `ffxiv__character`.`deleted` IS NULL GROUP BY `ffxiv__character`.`genderid`, `value` ORDER BY `count` DESC');
    }
    
    /**
     * Get data for statistics related to different groups
     *
     * @param array $data Array of gathered data
     *
     * @return void
     */
    private function getGroups(array &$data): void
    {
        #Get most popular estate locations
        $data['freecompany']['estate'] = Splitters::topAndBottom(Select::selectAll('SELECT `ffxiv__estate`.`area`, `ffxiv__estate`.`plot`, CONCAT(`ffxiv__estate`.`area`, \', plot \', `ffxiv__estate`.`plot`) AS `value`, count(`ffxiv__freecompany`.`estateid`) AS `count` FROM `ffxiv__freecompany` INNER JOIN `ffxiv__estate` ON `ffxiv__freecompany`.`estateid`=`ffxiv__estate`.`estateid` WHERE `ffxiv__freecompany`.`deleted` IS NULL AND `ffxiv__freecompany`.`estateid` IS NOT NULL GROUP BY `value` ORDER BY `count` DESC'), 20);
        #Get statistics by activity time
        $data['freecompany']['active'] = Select::selectAll('SELECT IF(`ffxiv__freecompany`.`recruitment`=1, \'Recruiting\', \'Not recruiting\') AS `recruiting`, SUM(IF(`ffxiv__freecompany`.`activeid` = 1, 1, 0)) AS `Always`, SUM(IF(`ffxiv__freecompany`.`activeid` = 2, 1, 0)) AS `Weekdays`, SUM(IF(`ffxiv__freecompany`.`activeid` = 3, 1, 0)) AS `Weekends` FROM `ffxiv__freecompany` INNER JOIN `ffxiv__timeactive` ON `ffxiv__freecompany`.`activeid`=`ffxiv__timeactive`.`activeid` WHERE `ffxiv__freecompany`.`deleted` IS NULL GROUP BY 1 ORDER BY 1 DESC');
        #Get statistics by activities
        $data['freecompany']['activities'] = Select::selectRow('SELECT  SUM(`Role-playing`)/COUNT(`freecompanyid`)*100 AS `Role-playing`, SUM(`Leveling`)/COUNT(`freecompanyid`)*100 AS `Leveling`, SUM(`Casual`)/COUNT(`freecompanyid`)*100 AS `Casual`, SUM(`Hardcore`)/COUNT(`freecompanyid`)*100 AS `Hardcore`, SUM(`Dungeons`)/COUNT(`freecompanyid`)*100 AS `Dungeons`, SUM(`Guildhests`)/COUNT(`freecompanyid`)*100 AS `Guildhests`, SUM(`Trials`)/COUNT(`freecompanyid`)*100 AS `Trials`, SUM(`Raids`)/COUNT(`freecompanyid`)*100 AS `Raids`, SUM(`PvP`)/COUNT(`freecompanyid`)*100 AS `PvP` FROM `ffxiv__freecompany` WHERE `deleted` IS NULL');
        arsort($data['freecompany']['activities']);
        #Get statistics by job search
        $data['freecompany']['jobDemand'] = Select::selectRow('SELECT SUM(`Tank`)/COUNT(`freecompanyid`)*100 AS `Tank`, SUM(`Healer`)/COUNT(`freecompanyid`)*100 AS `Healer`, SUM(`DPS`)/COUNT(`freecompanyid`)*100 AS `DPS`, SUM(`Crafter`)/COUNT(`freecompanyid`)*100 AS `Crafter`, SUM(`Gatherer`)/COUNT(`freecompanyid`)*100 AS `Gatherer` FROM `ffxiv__freecompany` WHERE `deleted` IS NULL');
        arsort($data['freecompany']['jobDemand']);
        #Get statistics for grand companies for characters
        $data['gc_characters'] = Select::selectAll(
            'SELECT COUNT(*) as `count`, `ffxiv__character`.`genderid`, `ffxiv__grandcompany`.`gcName`, `ffxiv__grandcompany_rank`.`gc_rank` FROM `ffxiv__character`
                                LEFT JOIN `ffxiv__grandcompany_rank` ON `ffxiv__character`.`gcrankid`=`ffxiv__grandcompany_rank`.`gcrankid`
                                LEFT JOIN `ffxiv__grandcompany` ON `ffxiv__grandcompany_rank`.`gcId`=`ffxiv__grandcompany`.`gcId`
                                WHERE `ffxiv__character`.`gcrankid` IS NOT NULL GROUP BY `ffxiv__character`.`genderid`, `ffxiv__grandcompany`.`gcName`, `ffxiv__grandcompany_rank`.`gc_rank` ORDER BY `count` DESC;
                    ');
        #Get statistics for grand companies for free companies
        $data['gc_companies'] = Select::selectAll('SELECT `ffxiv__grandcompany`.`gcName` AS `value`, count(`ffxiv__freecompany`.`grandcompanyid`) AS `count` FROM `ffxiv__freecompany` INNER JOIN `ffxiv__grandcompany` ON `ffxiv__freecompany`.`grandcompanyid`=`ffxiv__grandcompany`.`gcId` GROUP BY `value` ORDER BY `count` DESC');
        #City by free company
        $data['cities']['free_company'] = Select::selectAll('SELECT `ffxiv__estate`.`area` AS `value`, count(`ffxiv__freecompany`.`estateid`) AS `count` FROM `ffxiv__freecompany` INNER JOIN `ffxiv__estate` ON `ffxiv__freecompany`.`estateid`=`ffxiv__estate`.`estateid` WHERE `ffxiv__freecompany`.`estateid` IS NOT NULL AND `ffxiv__freecompany`.`deleted` IS NULL GROUP BY `value` ORDER BY `count` DESC');
        #Grand companies distribution (free companies)
        $data['cities']['gc_fc'] = Select::selectAll('SELECT `ffxiv__city`.`city`, `ffxiv__grandcompany`.`gcName` AS `value`, COUNT(`ffxiv__freecompany`.`freecompanyid`) AS `count` FROM `ffxiv__freecompany` LEFT JOIN `ffxiv__estate` ON `ffxiv__freecompany`.`estateid`=`ffxiv__estate`.`estateid` LEFT JOIN `ffxiv__city` ON `ffxiv__estate`.`cityid`=`ffxiv__city`.`cityid` LEFT JOIN `ffxiv__grandcompany_rank` ON `ffxiv__freecompany`.`grandcompanyid`=`ffxiv__grandcompany_rank`.`gcrankid` LEFT JOIN `ffxiv__grandcompany` ON `ffxiv__freecompany`.`grandcompanyid`=`ffxiv__grandcompany`.`gcId` WHERE `ffxiv__freecompany`.`deleted` IS NULL AND `ffxiv__freecompany`.`estateid` IS NOT NULL AND `ffxiv__grandcompany`.`gcName` IS NOT NULL GROUP BY `city`, `value` ORDER BY `count` DESC');
        #Free companies
        $data['servers']['Free Companies'] = Select::selectAll('SELECT `ffxiv__server`.`server` AS `value`, count(`ffxiv__freecompany`.`serverid`) AS `count` FROM `ffxiv__freecompany` INNER JOIN `ffxiv__server` ON `ffxiv__freecompany`.`serverid`=`ffxiv__server`.`serverid` WHERE `ffxiv__freecompany`.`deleted` IS NULL GROUP BY `value` ORDER BY `count` DESC');
        #Linkshells
        $data['servers']['Linkshells'] = Select::selectAll('SELECT `ffxiv__server`.`server` AS `value`, count(`ffxiv__linkshell`.`serverid`) AS `count` FROM `ffxiv__linkshell` INNER JOIN `ffxiv__server` ON `ffxiv__linkshell`.`serverid`=`ffxiv__server`.`serverid` WHERE `ffxiv__linkshell`.`crossworld` = 0 AND `ffxiv__linkshell`.`deleted` IS NULL GROUP BY `value` ORDER BY `count` DESC');
        #Crossworld linkshells
        $data['servers']['crossworldlinkshell'] = Select::selectAll('SELECT `ffxiv__server`.`datacenter` AS `value`, count(`ffxiv__linkshell`.`serverid`) AS `count` FROM `ffxiv__linkshell` INNER JOIN `ffxiv__server` ON `ffxiv__linkshell`.`serverid`=`ffxiv__server`.`serverid` WHERE `ffxiv__linkshell`.`crossworld` = 1 AND `ffxiv__linkshell`.`deleted` IS NULL GROUP BY `value` ORDER BY `count` DESC');
        #PvP teams
        $data['servers']['pvpteam'] = Select::selectAll('SELECT `ffxiv__server`.`datacenter` AS `value`, count(`ffxiv__pvpteam`.`datacenterid`) AS `count` FROM `ffxiv__pvpteam` INNER JOIN `ffxiv__server` ON `ffxiv__pvpteam`.`datacenterid`=`ffxiv__server`.`serverid` WHERE `ffxiv__pvpteam`.`deleted` IS NULL GROUP BY `value` ORDER BY `count` DESC');
        #Get the most popular crests for companies
        $data['freecompany']['crests'] = AbstractTrackerEntity::cleanCrestResults(Select::selectAll('SELECT COUNT(*) AS `count`, `crest_part_1`, `crest_part_2`, `crest_part_3` FROM `ffxiv__freecompany` GROUP BY `crest_part_1`, `crest_part_2`, `crest_part_3` ORDER BY `count` DESC LIMIT 20;'));
        #Get the most popular crests for PvP Teams
        $data['pvpteam']['crests'] = AbstractTrackerEntity::cleanCrestResults(Select::selectAll('SELECT COUNT(*) AS `count`, `crest_part_1`, `crest_part_2`, `crest_part_3` FROM `ffxiv__pvpteam` GROUP BY `crest_part_1`, `crest_part_2`, `crest_part_3` ORDER BY `count` DESC LIMIT 20;'));
    }
    
    /**
     * Get data for achievements' statistics
     *
     * @param array $data Array of gathered data
     *
     * @return void
     */
    private function getAchievements(array &$data): void
    {
        #Get achievements statistics
        $data['achievements'] = Select::selectAll(
            'SELECT \'achievement\' as `type`, `category`, `achievementid` AS `id`, `icon`, `name`, `earnedby` AS `count`
                    FROM `ffxiv__achievement`
                    WHERE `ffxiv__achievement`.`category` IS NOT NULL AND `earnedby`>0 ORDER BY `count`;'
        );
        #Split achievements by categories
        $data['achievements'] = Splitters::splitByKey($data['achievements'], 'category');
        #Get only the top 20 for each category
        foreach ($data['achievements'] as $key => $category) {
            $data['achievements'][$key] = \array_slice($category, 0, 20);
        }
        #Get the most and least popular titles
        $data['titles'] = Splitters::topAndBottom(Select::selectAll('SELECT COUNT(*) as `count`, `ffxiv__achievement`.`title`, `ffxiv__achievement`.`achievementid` FROM `ffxiv__character` LEFT JOIN `ffxiv__achievement` ON `ffxiv__achievement`.`achievementid`=`ffxiv__character`.`titleid` WHERE `ffxiv__character`.`titleid` IS NOT NULL GROUP BY `titleid` ORDER BY `count` DESC;'), 20);
        
    }
    
    /**
     * Get data statistics linked to dates
     *
     * @param array $data Array of gathered data
     *
     * @return void
     */
    private function getTimelines(array &$data): void
    {
        #Get namedays timeline. Using custom SQL, since need special order by `namedayid`, instead of by `count`
        $data['namedays'] = Select::selectAll('SELECT `ffxiv__nameday`.`nameday` AS `value`, COUNT(`ffxiv__character`.`namedayid`) AS `count` FROM `ffxiv__character` INNER JOIN `ffxiv__nameday` ON `ffxiv__character`.`namedayid`=`ffxiv__nameday`.`namedayid` GROUP BY `ffxiv__nameday`.`namedayid` ORDER BY `count` DESC;');
        #Timeline of entities formation, updates, etc.
        $data['timelines'] = Select::selectAll(
            'SELECT `registered` AS `date`, COUNT(*) AS `count`, \'characters_registered\' as `type` FROM `ffxiv__character` GROUP BY `date`
                            UNION
                            SELECT `deleted` AS `date`, COUNT(*) AS `count`, \'characters_deleted\' as `type` FROM `ffxiv__character` WHERE `deleted` IS NOT NULL GROUP BY `date`
                            UNION
                            SELECT `privated` AS `date`, COUNT(*) AS `count`, \'characters_privated\' as `type` FROM `ffxiv__character` WHERE `privated` IS NOT NULL GROUP BY `date`
                            UNION
                            SELECT `formed` AS `date`, COUNT(*) AS `count`, \'free_companies_formed\' as `type` FROM `ffxiv__freecompany` GROUP BY `date`
                            UNION
                            SELECT `registered` AS `date`, COUNT(*) AS `count`, \'free_companies_registered\' as `type` FROM `ffxiv__freecompany` GROUP BY `date`
                            UNION
                            SELECT `deleted` AS `date`, COUNT(*) AS `count`, \'free_companies_deleted\' as `type` FROM `ffxiv__freecompany` WHERE `deleted` IS NOT NULL GROUP BY `date`
                            UNION
                            SELECT `formed` AS `date`, COUNT(*) AS `count`, \'pvp_teams_formed\' as `type` FROM `ffxiv__pvpteam` WHERE `formed` IS NOT NULL GROUP BY `date`
                            UNION
                            SELECT `registered` AS `date`, COUNT(*) AS `count`, \'pvp_teams_registered\' as `type` FROM `ffxiv__pvpteam` GROUP BY `date`
                            UNION
                            SELECT `deleted` AS `date`, COUNT(*) AS `count`, \'pvp_teams_deleted\' as `type` FROM `ffxiv__pvpteam` WHERE `deleted` IS NOT NULL GROUP BY `date`
                            UNION
                            SELECT `formed` AS `date`, COUNT(*) AS `count`, \'linkshells_formed\' as `type` FROM `ffxiv__linkshell` WHERE `formed` IS NOT NULL GROUP BY `date`
                            UNION
                            SELECT `registered` AS `date`, COUNT(*) AS `count`, \'linkshells_registered\' as `type` FROM `ffxiv__linkshell` GROUP BY `date`
                            UNION
                            SELECT `deleted` AS `date`, COUNT(*) AS `count`, \'linkshells_deleted\' as `type` FROM `ffxiv__linkshell` WHERE `deleted` IS NOT NULL GROUP BY `date`;'
        );
        $data['timelines'] = Splitters::splitByKey($data['timelines'], 'date');
        foreach ($data['timelines'] as $date => $datapoint) {
            $data['timelines'][$date] = Converters::MultiToSingle(Editors::DigitToKey($datapoint, 'type', true), 'count');
        }
        krsort($data['timelines']);
    }
    
    /**
     * Get data for potential data bugs
     *
     * @param array $data Array of gathered data
     *
     * @return void
     */
    private function getBugs(array &$data): void
    {
        #Characters with no clan/race
        $data['bugs']['noClan'] = Select::selectAll('SELECT `characterid` AS `id`, `name`, `avatar` AS `icon`, \'character\' AS `type` FROM `ffxiv__character` WHERE `clanid` IS NULL AND `deleted` IS NULL AND `privated` IS NULL ORDER BY `updated`, `name`;');
        #Characters with no avatar
        $data['bugs']['noAvatar'] = Select::selectAll('SELECT `characterid` AS `id`, `name`, `avatar` AS `icon`, \'character\' AS `type` FROM `ffxiv__character` WHERE `avatar` LIKE \'defaultf%\' AND `deleted` IS NULL AND `privated` IS NULL ORDER BY `updated`, `name`;');
        #Groups with no members
        $data['bugs']['noMembers'] = AbstractTrackerEntity::cleanCrestResults(Select::selectAll(
            'SELECT `freecompanyid` AS `id`, `name`, \'freecompany\' AS `type`, `crest_part_1`, `crest_part_2`, `crest_part_3`, `grandcompanyid` FROM `ffxiv__freecompany` as `fc` WHERE `deleted` IS NULL AND `freecompanyid` NOT IN (SELECT `freecompanyid` FROM `ffxiv__freecompany_character` WHERE `freecompanyid`=`fc`.`freecompanyid` AND `current`=1)
                        UNION
                        SELECT `linkshellid` AS `id`, `name`, IF(`crossworld`=1, \'crossworldlinkshell\', \'linkshell\') AS `type`, null as `crest_part_1`, null as `crest_part_2`, null as `crest_part_3`, null as `grandcompanyid` FROM `ffxiv__linkshell` as `ls` WHERE `deleted` IS NULL AND `linkshellid` NOT IN (SELECT `linkshellid` FROM `ffxiv__linkshell_character` WHERE `linkshellid`=`ls`.`linkshellid` AND `current`=1)
                        UNION
                        SELECT `pvpteamid` AS `id`, `name`, \'pvpteam\' AS `type`, `crest_part_1`, `crest_part_2`, `crest_part_3`, null as `grandcompanyid` FROM `ffxiv__pvpteam` as `pvp` WHERE `deleted` IS NULL AND `pvpteamid` NOT IN (SELECT `pvpteamid` FROM `ffxiv__pvpteam_character` WHERE `pvpteamid`=`pvp`.`pvpteamid` AND `current`=1)
                        ORDER BY `name`;'
        ));
        #Get entities with duplicate names
        $duplicateNames = Select::selectAll(
            'SELECT \'character\' AS `type`, `chartable`.`characterid` AS `id`, `name`, `avatar` as `icon`, `userid`, NULL as `crest_part_1`, NULL as `crest_part_2`, NULL as `crest_part_3`, `server`, `datacenter` FROM `ffxiv__character` as `chartable` LEFT JOIN `uc__user_to_ff_character` ON `uc__user_to_ff_character`.`characterid`=`chartable`.`characterid` LEFT JOIN `ffxiv__server` ON `ffxiv__server`.`serverid`=`chartable`.`serverid` WHERE `deleted` IS NULL AND `privated` IS NULL AND (SELECT COUNT(*) as `count` FROM `ffxiv__character` WHERE `ffxiv__character`.`name`=`chartable`.`name` AND `ffxiv__character`.`serverid`=`chartable`.`serverid` AND `deleted` is NULL)>1
                            UNION ALL
                            SELECT \'freecompany\' AS `type`, `freecompanyid` AS `id`, `name`, NULL as `icon`, NULL as `userid`, `crest_part_1`, `crest_part_2`, `crest_part_3`, `server`, `datacenter`  FROM `ffxiv__freecompany` as `fctable` LEFT JOIN `ffxiv__server` ON `ffxiv__server`.`serverid`=`fctable`.`serverid` WHERE `deleted` is NULL AND (SELECT COUNT(*) as `count` FROM `ffxiv__freecompany` WHERE `ffxiv__freecompany`.`name`= BINARY `fctable`.`name` AND `ffxiv__freecompany`.`serverid`=`fctable`.`serverid` AND `deleted` is NULL)>1
                            UNION ALL
                            SELECT \'pvpteam\' AS `type`, `pvpteamid` AS `id`, `name`, NULL as `icon`, NULL as `userid`, `crest_part_1`, `crest_part_2`, `crest_part_3`, `server`, `datacenter`  FROM `ffxiv__pvpteam` as `pvptable` LEFT JOIN `ffxiv__server` ON `ffxiv__server`.`serverid`=`pvptable`.`datacenterid` WHERE `deleted` is NULL AND (SELECT COUNT(*) as `count` FROM `ffxiv__pvpteam` WHERE `ffxiv__pvpteam`.`name`= BINARY `pvptable`.`name` AND `ffxiv__pvpteam`.`datacenterid`=`pvptable`.`datacenterid` AND `deleted` is NULL)>1
                            UNION ALL
                            SELECT IF(`crossworld` = 0, \'linkshell\', \'crossworldlinkshell\') AS `type`, `linkshellid` AS `id`, `name`, NULL as `icon`, NULL as `userid`, NULL as `crest_part_1`, NULL as `crest_part_2`, NULL as `crest_part_3`, `server`, `datacenter`  FROM `ffxiv__linkshell` as `lstable` LEFT JOIN `ffxiv__server` ON `ffxiv__server`.`serverid`=`lstable`.`serverid` WHERE `deleted` is NULL AND (SELECT COUNT(*) as `count` FROM `ffxiv__linkshell` WHERE `ffxiv__linkshell`.`name`= BINARY `lstable`.`name` AND `ffxiv__linkshell`.`serverid`=`lstable`.`serverid` AND `deleted` is NULL AND `ffxiv__linkshell`.`crossworld`=`lstable`.`crossworld`)>1;'
        );
        #Split by entity type
        $data['bugs']['duplicateNames'] = Splitters::splitByKey($duplicateNames, 'type', keepKey: true);
        foreach ($data['bugs']['duplicateNames'] as $entityType => $namesData) {
            #Split by server/datacenter
            $data['bugs']['duplicateNames'][$entityType] = Splitters::splitByKey($namesData, (in_array($entityType, ['pvpteam', 'crosswordlinkshell']) ? 'datacenter' : 'server'));
            foreach ($data['bugs']['duplicateNames'][$entityType] as $server => $serverData) {
                #Split by name
                $data['bugs']['duplicateNames'][$entityType][$server] = Splitters::splitByKey($serverData, 'name', keepKey: true, caseInsensitive: true);
                foreach ($data['bugs']['duplicateNames'][$entityType][$server] as $name => $nameData) {
                    if (in_array($entityType, ['freecompany', 'pvpteam'])) {
                        $nameData = AbstractTrackerEntity::cleanCrestResults($nameData);
                    }
                    foreach ($nameData as $key => $duplicates) {
                        #Clean up
                        unset($duplicates['crest_part_1'], $duplicates['crest_part_2'], $duplicates['crest_part_3']);
                        if (in_array($entityType, ['crossworldlinkshell', 'pvpteam'])) {
                            unset($duplicates['server']);
                        } else {
                            unset($duplicates['datacenter']);
                        }
                        #Update array
                        $data['bugs']['duplicateNames'][$entityType][$server][$name][$key] = $duplicates;
                    }
                }
            }
        }
    }
    
    /**
     * Get data for uncategorized statistics
     *
     * @param array $data Array of gathered data
     *
     * @return void
     */
    private function getOthers(array &$data): void
    {
        #Communities
        $data['other']['communities'] = Select::selectAll(/** @lang MySQL */ '
                            (SELECT \'Free Company\' AS `type`, \'No community\' AS `value`, COUNT(*) as `count` FROM `ffxiv__freecompany` WHERE `deleted` IS NULL AND `communityid` IS NULL)
                            UNION ALL
                            (SELECT \'Free Company\' AS `type`, \'Community\' AS `value`, COUNT(*) as `count` FROM `ffxiv__freecompany` WHERE `deleted` IS NULL AND `communityid` IS NOT NULL)
                            UNION ALL
                            (SELECT \'PvP Team\' AS `type`, \'No community\' AS `value`, COUNT(*) as `count` FROM `ffxiv__pvpteam` WHERE `deleted` IS NULL AND `communityid` IS NULL)
                            UNION ALL
                            (SELECT \'PvP Team\' AS `type`, \'Community\' AS `value`, COUNT(*) as `count` FROM `ffxiv__pvpteam` WHERE `deleted` IS NULL AND `communityid` IS NOT NULL)
                            UNION ALL
                            (SELECT \'Linkshell\' AS `type`, \'No community\' AS `value`, COUNT(*) as `count` FROM `ffxiv__linkshell` WHERE `deleted` IS NULL AND `communityid` IS NULL AND `crossworld`=0)
                            UNION ALL
                            (SELECT \'Linkshell\' AS `type`, \'Community\' AS `value`, COUNT(*) as `count` FROM `ffxiv__linkshell` WHERE `deleted` IS NULL AND `communityid` IS NOT NULL AND `crossworld`=0)
                            UNION ALL
                            (SELECT \'Crossworld Linkshell\' AS `type`, \'No community\' AS `value`, COUNT(*) as `count` FROM `ffxiv__linkshell` WHERE `deleted` IS NULL AND `communityid` IS NULL AND `crossworld`=1)
                            UNION ALL
                            (SELECT \'Crossworld Linkshell\' AS `type`, \'Community\' AS `value`, COUNT(*) as `count` FROM `ffxiv__linkshell` WHERE `deleted` IS NULL AND `communityid` IS NOT NULL AND `crossworld`=1)
                            ORDER BY `type`, `value`
                    ');
        #Deleted entities statistics
        $data['other']['entities'] = Select::selectAll(/** @lang MySQL */ '
                            (SELECT \'Active Character\' AS `value`, COUNT(*) AS `count` FROM `ffxiv__character` WHERE `deleted` IS NULL)
                            UNION ALL
                            (SELECT \'Deleted Character\' AS `value`, COUNT(*) AS `count` FROM `ffxiv__character` WHERE `deleted` IS NOT NULL)
                            UNION ALL
                            (SELECT \'Active Free Company\' AS `value`, COUNT(*) AS `count` FROM `ffxiv__freecompany` WHERE `deleted` IS NULL)
                            UNION ALL
                            (SELECT \'Deleted Free Company\' AS `value`, COUNT(*) AS `count` FROM `ffxiv__freecompany` WHERE `deleted` IS NOT NULL)
                            UNION ALL
                            (SELECT \'Active PvP Team\' AS `value`, COUNT(*) AS `count` FROM `ffxiv__pvpteam` WHERE `deleted` IS NULL)
                            UNION ALL
                            (SELECT \'Deleted PvP Team\' AS `value`, COUNT(*) AS `count` FROM `ffxiv__pvpteam` WHERE `deleted` IS NOT NULL)
                            UNION ALL
                            (SELECT \'Active Linkshell\' AS `value`, COUNT(*) AS `count` FROM `ffxiv__linkshell` WHERE `deleted` IS NULL AND `crossworld`=0)
                            UNION ALL
                            (SELECT \'Deleted Linkshell\' AS `value`, COUNT(*) AS `count` FROM `ffxiv__linkshell` WHERE `deleted` IS NOT NULL AND `crossworld`=0)
                            UNION ALL
                            (SELECT \'Active Crossworld Linkshell\' AS `value`, COUNT(*) AS `count` FROM `ffxiv__linkshell` WHERE `deleted` IS NULL AND `crossworld`=1)
                            UNION ALL
                            (SELECT \'Deleted Crossworld Linkshell\' AS `value`, COUNT(*) AS `count` FROM `ffxiv__linkshell` WHERE `deleted` IS NOT NULL AND `crossworld`=1)
                            ORDER BY `count` DESC;
                    ');
        #Number of updated entities in the last 30 days
        $data['updates_stats'] = Select::selectAll(
        /** @lang MariaDB */ '(SELECT DATE(`updated`) AS `date`, COUNT(*) AS `count`, \'characters\' as `type` FROM `ffxiv__character` GROUP BY `date` ORDER BY `date` DESC LIMIT 30)
                            UNION
                            (SELECT DATE(`updated`) AS `date`, COUNT(*) AS `count`, \'free_companies\' as `type` FROM `ffxiv__freecompany` GROUP BY `date` ORDER BY `date` DESC LIMIT 30)
                            UNION
                            (SELECT DATE(`updated`) AS `date`, COUNT(*) AS `count`, \'pvp_teams\' as `type` FROM `ffxiv__pvpteam` GROUP BY `date` ORDER BY `date` DESC LIMIT 30)
                            UNION
                            (SELECT DATE(`updated`) AS `date`, COUNT(*) AS `count`, \'linkshells\' as `type` FROM `ffxiv__linkshell` GROUP BY `date` ORDER BY `date` DESC LIMIT 30);'
        );
        $data['updates_stats'] = Splitters::splitByKey($data['updates_stats'], 'date');
        foreach ($data['updates_stats'] as $date => $datapoint) {
            $data['updates_stats'][$date] = Converters::MultiToSingle(Editors::DigitToKey($datapoint, 'type', true), 'count');
        }
        krsort($data['updates_stats']);
        $data['updates_stats'] = \array_slice($data['updates_stats'], 0, 30);
    }
    
    /**
     * Adds jobs for all entities considered as potential bugs
     *
     * @param array $data Array of gathered data
     *
     * @return void
     * @throws \Exception
     */
    private function scheduleBugs(array $data): void
    {
        #These may be because of temporary issues on the parser or Lodestone side, so schedule them for update
        $cron = new TaskInstance();
        foreach ($data['bugs']['noClan'] as $character) {
            $cron->settingsFromArray(['task' => 'ffUpdateEntity', 'arguments' => [(string)$character['id'], 'character'], 'message' => 'Updating character with ID '.$character['id']])->add();
        }
        foreach ($data['bugs']['noAvatar'] as $character) {
            $cron->settingsFromArray(['task' => 'ffUpdateEntity', 'arguments' => [(string)$character['id'], 'character'], 'message' => 'Updating character with ID '.$character['id']])->add();
        }
        foreach ($data['bugs']['noMembers'] as $group) {
            $cron->settingsFromArray(['task' => 'ffUpdateEntity', 'arguments' => [(string)$group['id'], $group['type']], 'message' => 'Updating group with ID '.$group['id']])->add();
        }
        foreach ($data['bugs']['duplicateNames'] as $servers) {
            foreach ($servers as $server) {
                foreach ($server as $names) {
                    foreach ($names as $duplicate) {
                        $cron->settingsFromArray(['task' => 'ffUpdateEntity', 'arguments' => [(string)$duplicate['id'], $duplicate['type']], 'message' => 'Updating entity with ID '.$duplicate['id']])->add();
                    }
                }
            }
        }
    }
}