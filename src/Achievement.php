<?php
declare(strict_types = 1);

namespace Simbiat\FFXIV;

use Simbiat\Cron\TaskInstance;
use Simbiat\Database\Query;
use Simbiat\Website\Config;
use Simbiat\Website\Errors;
use Simbiat\Website\Images;

/**
 * Class representing a FFXIV achievement
 */
class Achievement extends AbstractTrackerEntity
{
    #Custom properties
    protected const string ENTITY_TYPE = 'achievement';
    public int $updated;
    public int $registered;
    public ?string $category = null;
    public ?string $subcategory = null;
    public ?string $icon = null;
    public ?string $how_to = null;
    public ?string $db_id = null;
    public array $rewards = [];
    public array $characters = [];
    
    /**
     * Function to get initial data from DB
     * @throws \Exception
     */
    protected function getFromDB(): array
    {
        #Get general information
        $data = Query::query('SELECT * FROM `ffxiv__achievement` WHERE `ffxiv__achievement`.`achievement_id` = :id', [':id' => $this->id], return: 'row');
        #Return empty if nothing was found
        if (empty($data)) {
            return [];
        }
        #Get last characters with this achievement
        $data['characters'] = Query::query('SELECT * FROM (SELECT \'character\' AS `type`, `ffxiv__character`.`character_id` AS `id`, `ffxiv__character`.`name`, `ffxiv__character`.`avatar` AS `icon` FROM `ffxiv__character_achievement` LEFT JOIN `ffxiv__character` ON `ffxiv__character`.`character_id` = `ffxiv__character_achievement`.`character_id` WHERE `ffxiv__character_achievement`.`achievement_id` = :id ORDER BY `ffxiv__character_achievement`.`time` DESC LIMIT 50) t ORDER BY `name`', [':id' => $this->id], return: 'all');
        #Register for an update if old enough or category or how_to or db_id are empty. Also check that this is not a bot.
        if (empty($_SESSION['UA']['bot']) && !empty($data['characters']) && (empty($data['category']) || empty($data['subcategory']) || empty($data['how_to']) || empty($data['db_id']) || (time() - strtotime($data['updated'])) >= 31536000)) {
            new TaskInstance()->settingsFromArray(['task' => 'ffUpdateEntity', 'arguments' => [(string)$this->id, 'achievement'], 'message' => 'Updating achievement with ID '.$this->id, 'priority' => 2])->add();
        }
        return $data;
    }
    
    /**
     * Get data from Lodestone
     *
     * @param bool $allowSleep Whether to wait in case Lodestone throttles the request (that is throttle on our side)
     *
     * @return string|array
     * @throws \Exception
     */
    public function getFromLodestone(bool $allowSleep = false): string|array
    {
        #Get the data that we have
        $achievement = $this->getFromDB();
        if (empty($achievement['name'])) {
            return ['404' => true, 'reason' => 'Achievement with ID `'.$this->id.'` is not found on Tracker'];
        }
        #Cache Lodestone
        $lodestone = new Lodestone();
        #If we do not have db_id already - try to get one
        if (empty($achievement['db_id'])) {
            $achievement['db_id'] = $this->getDBID($achievement['name']);
        }
        #Somewhat simpler and faster processing if we have db_id already
        if (!empty($achievement['db_id'])) {
            $data = $lodestone->getAchievementFromDB($achievement['db_id'])->getResult();
            $data = $data['database']['achievement'][$achievement['db_id']];
            unset($data['time']);
            $data['db_id'] = $achievement['db_id'];
            $data['id'] = $this->id;
            return $data;
        }
        if (empty($achievement['characters'])) {
            return ['404' => true];
        }
        #Iterrate list
        foreach ($achievement['characters'] as $char) {
            $data = $lodestone->getCharacterAchievements($char['id'], (int)$this->id)->getResult();
            #Take a pause if we were throttled, and pause is allowed
            if (!empty($lodestone->getLastError()['error']) && preg_match('/Lodestone has throttled the request, 429/', $lodestone->getLastError()['error']) === 1) {
                if ($allowSleep) {
                    sleep(60);
                }
                return 'Request throttled by Lodestone';
            }
            if (!empty($data['characters'][$char['id']]['achievements'][$this->id]) && \is_array($data['characters'][$char['id']]['achievements'][$this->id])) {
                #Try to get achievement ID as seen in Lodestone database (play guide)
                $data['characters'][$char['id']]['achievements'][$this->id]['db_id'] = $this->getDBID($data['characters'][$char['id']]['achievements'][$this->id]['name']);
                #Remove time
                unset($data['characters'][$char['id']]['achievements'][$this->id]['time']);
                $data = $data['characters'][$char['id']]['achievements'][$this->id];
                $data['id'] = $this->id;
                return $data;
            }
        }
        return ['404' => true];
    }
    
    /**
     * Helper function to get db_id from Lodestone based on the achievement name
     * @param string $search_for
     *
     * @return string|null
     */
    private function getDBID(string $search_for): string|null
    {
        $db_search_result = new Lodestone()->searchDatabase('achievement', 0, 0, $search_for)->getResult();
        #Remove counts elements from achievement database
        unset($db_search_result['database']['achievement']['pageCurrent'], $db_search_result['database']['achievement']['pageTotal'], $db_search_result['database']['achievement']['total']);
        if (empty($db_search_result)) {
            return null;
        }
        #Flip the array of achievements (if any) to ease searching for the right element
        $db_search_result['database']['achievement'] = array_flip(array_combine(array_keys($db_search_result['database']['achievement']), array_column($db_search_result['database']['achievement'], 'name')));
        if (!empty($db_search_result['database']['achievement'][$search_for])) {
            return $db_search_result['database']['achievement'][$search_for];
        }
        return null;
    }
    
    /**
     * Function to do processing of DB data
     * @param array $fromDB
     *
     * @return void
     */
    protected function process(array $fromDB): void
    {
        $this->name = $fromDB['name'];
        $this->updated = strtotime($fromDB['updated']);
        $this->registered = strtotime($fromDB['registered']);
        $this->category = $fromDB['category'];
        $this->subcategory = $fromDB['subcategory'];
        $this->icon = $fromDB['icon'];
        $this->how_to = $fromDB['how_to'];
        $this->db_id = $fromDB['db_id'];
        $this->rewards = [
            'points' => (int)$fromDB['points'],
            'title' => $fromDB['title'],
            'item' => [
                'name' => $fromDB['item'],
                'icon' => $fromDB['item_icon'],
                'id' => $fromDB['item_id'],
            ],
        ];
        $this->characters = [
            'total' => (int)$fromDB['earned_by'],
            'last' => $fromDB['characters'],
        ];
    }
    
    /**
     * Function to update the entity in DB
     * @return bool
     */
    protected function updateDB(): bool
    {
        
        #Prepare bindings for actual update
        $bindings = [];
        $bindings[':achievement_id'] = $this->id;
        $bindings[':name'] = $this->lodestone['name'];
        $bindings[':icon'] = self::removeLodestoneDomain($this->lodestone['icon']);
        #Download icon
        $webp = Images::download($this->lodestone['icon'], Config::$icons.$bindings[':icon']);
        if ($webp) {
            $bindings[':icon'] = str_replace('.png', '.webp', $bindings[':icon']);
        }
        $bindings[':points'] = $this->lodestone['points'];
        $bindings[':category'] = $this->lodestone['category'];
        $bindings[':subcategory'] = $this->lodestone['subcategory'];
        if (empty($this->lodestone['how_to'])) {
            $bindings[':how_to'] = [NULL, 'null'];
        } else {
            $bindings[':how_to'] = $this->lodestone['how_to'];
        }
        if (empty($this->lodestone['title'])) {
            $bindings[':title'] = [NULL, 'null'];
        } else {
            $bindings[':title'] = $this->lodestone['title'];
        }
        if (empty($this->lodestone['item']['name'])) {
            $bindings[':item'] = [NULL, 'null'];
        } else {
            $bindings[':item'] = $this->lodestone['item']['name'];
        }
        if (empty($this->lodestone['item']['icon'])) {
            $bindings[':item_icon'] = [NULL, 'null'];
        } else {
            $bindings[':item_icon'] = self::removeLodestoneDomain($this->lodestone['item']['icon']);
            #Download icon
            $webp = Images::download($this->lodestone['item']['icon'], Config::$icons.$bindings[':item_icon']);
            if ($webp) {
                $bindings[':item_icon'] = str_replace('.png', '.webp', $bindings[':item_icon']);
            }
        }
        if (empty($this->lodestone['item']['id'])) {
            $bindings[':item_id'] = [NULL, 'null'];
        } else {
            $bindings[':item_id'] = $this->lodestone['item']['id'];
        }
        if (empty($this->lodestone['db_id'])) {
            $bindings[':db_id'] = [NULL, 'null'];
        } else {
            $bindings[':db_id'] = $this->lodestone['db_id'];
        }
        try {
            return Query::query('INSERT INTO `ffxiv__achievement` SET `achievement_id`=:achievement_id, `name`=:name, `icon`=:icon, `points`=:points, `category`=:category, `subcategory`=:subcategory, `how_to`=:how_to, `title`=:title, `item`=:item, `item_icon`=:item_icon, `item_id`=:item_id, `db_id`=:db_id ON DUPLICATE KEY UPDATE `achievement_id`=:achievement_id, `name`=:name, `icon`=:icon, `points`=:points, `category`=:category, `subcategory`=:subcategory, `how_to`=:how_to, `title`=:title, `item`=:item, `item_icon`=:item_icon, `item_id`=:item_id, `db_id`=:db_id, `updated`=CURRENT_TIMESTAMP()', $bindings);
        } catch (\Throwable $e) {
            Errors::error_log($e, 'achievement_id: '.$this->id);
            return false;
        }
    }
}