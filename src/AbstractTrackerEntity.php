<?php
declare(strict_types = 1);

namespace Simbiat\FFXIV;

use Simbiat\Cron\TaskInstance;
use Simbiat\Database\Query;
use Simbiat\Website\Config;
use Simbiat\Website\Errors;
use Simbiat\Website\Images;
use function dirname;
use function is_array;
use function sprintf;

/**
 * Generic class for FFXIV entities
 */
abstract class AbstractTrackerEntity
{
    #Flag to indicate whether there was an attempt to get data within this object. Meant to help reduce reuse of the same object for different sets of data
    protected bool $attempted = false;
    #If ID was retrieved, this needs to not be null
    public ?string $id = null;
    #Format for IDs
    protected string $id_format = '/^\d+$/m';
    #Debug flag
    protected bool $debug = false;
    protected const ENTITY_TYPE = 'character';
    public string $name = '';
    
    protected null|array $lodestone = null;
    
    /**
     * @param string|int|null $id    ID of an entity
     * @param bool            $debug Flag to enable debug mode
     */
    final public function __construct(string|int|null $id = null, bool $debug = false)
    {
        #Set debug flag
        $this->debug = $debug;
        #If ID was provided - set it as well
        if (!empty($id)) {
            $this->setId($id);
        } elseif ($id !== null) {
            throw new \UnexpectedValueException('ID can\'t be empty.');
        }
    }
    
    /**
     * Set entity ID
     * @param string|int $id
     *
     * @return $this
     */
    public function setId(string|int $id): self
    {
        #Convert to string for consistency
        $id = (string)$id;
        if (preg_match($this->id_format, $id) !== 1) {
            throw new \UnexpectedValueException('ID `'.$id.'` for entity `'.get_class($this).'` has incorrect format.');
        }
        $this->id = $id;
        return $this;
    }
    
    /**
     * Get entity properties
     * @return $this
     */
    final public function get(): self
    {
        #Set the flag that we have tried to get data
        $this->attempted = true;
        try {
            #Set ID
            if ($this->id === null) {
                throw new \UnexpectedValueException('ID can\'t be empty.');
            }
            #Get data
            $result = $this->getFromDB();
            if (empty($result)) {
                #Reset ID
                $this->id = null;
            } else {
                $this->process($result);
            }
        } catch (\Throwable $exception) {
            $error = $exception->getMessage().$exception->getTraceAsString();
            Errors::error_log($exception);
            #Rethrow exception if using debug mode
            if ($this->debug) {
                die('<pre>'.$error.'</pre>');
            }
        }
        return $this;
    }
    
    /**
     * Get the data in an array
     * @return array
     */
    final public function getArray(): array
    {
        #If data was not retrieved yet - attempt to
        if (!$this->attempted) {
            try {
                $this->get();
            } catch (\Throwable) {
                return [];
            }
        }
        $array = get_mangled_object_vars($this);
        #Remove private and protected properties
        foreach ($array as $key => $value) {
            if (preg_match('/^\x00/u', $key) === 1) {
                unset($array[$key]);
            }
        }
        return $array;
    }
    
    /**
     * Function to get initial data from DB
     * @throws \Exception
     */
    abstract protected function getFromDB(): array;
    
    /**
     * Get entity data from Lodestone
     * @param bool $allow_sleep Whether to wait in case Lodestone throttles the request (that is throttle on our side)
     *
     * @return string|array
     */
    abstract public function getFromLodestone(bool $allow_sleep = false): string|array;
    
    /**
     * Function to do processing
     * @param array $from_db
     *
     * @return void
     */
    abstract protected function process(array $from_db): void;
    
    /**
     * Function to update the entity in DB
     * @return bool
     */
    abstract protected function updateDB(): bool;
    
    /**
     * Update the entity
     * @param bool $allow_sleep Flag to allow sleep if Lodestone is throttling us
     *
     * @return string|bool
     */
    public function update(bool $allow_sleep = false): string|bool
    {
        #Check if ID was set
        if ($this->id === null) {
            return false;
        }
        $id_column = match ($this::ENTITY_TYPE) {
            'character' => 'character_id',
            'achievement' => 'achievement_id',
            'freecompany' => 'fc_id',
            'linkshell' => 'ls_id',
            'pvpteam' => 'pvp_id',
        };
        #Check if we have not updated before
        try {
            $updated = Query::query('SELECT `updated` FROM `ffxiv__'.$this::ENTITY_TYPE.'` WHERE `'.$id_column.'` = :id', [':id' => $this->id], return: 'value');
        } catch (\Throwable $exception) {
            Errors::error_log($exception, debug: $this->debug);
            return $exception->getMessage()."\n".$exception->getTraceAsString();
        }
        #Check if it has not been updated recently (10 minutes, to protect from potential abuse)
        if (isset($updated) && (time() - strtotime($updated)) < 600) {
            #Return entity type
            return true;
        }
        #Try to get data from Lodestone, if not already taken
        if (!is_array($this->lodestone)) {
            try {
                $temp_lodestone = $this->getFromLodestone($allow_sleep);
            } catch (\Throwable $exception) {
                Errors::error_log($exception, 'Failed to get '.$this::ENTITY_TYPE.' with ID '.$this->id, debug: $this->debug);
                return $exception->getMessage()."\r\n".$exception->getTraceAsString();
            }
            if (!is_array($temp_lodestone)) {
                return $temp_lodestone;
            }
            $this->lodestone = $temp_lodestone;
        }
        #If we got 404, return true. If an entity is to be removed, it's done during getFromLodestone()
        if (isset($this->lodestone['404']) && $this->lodestone['404'] === true) {
            return true;
        }
        #Characters can mark their profiles as private on Lodestone since Dawntrail
        if ($this::ENTITY_TYPE === 'character' && isset($this->lodestone['private']) && $this->lodestone['private'] === true) {
            return true;
        }
        unset($this->lodestone['404']);
        if (empty($this->lodestone['name'])) {
            return 'No name found for '.$this::ENTITY_TYPE.' ID `'.$this->id.'`';
        }
        return $this->updateDB();
    }
    
    /**
     * To be called from API to allow entity updates
     * @return bool|array|string
     */
    public function updateFromApi(): bool|array|string
    {
        if ($_SESSION['user_id'] === 1) {
            return ['http_error' => 403, 'reason' => 'Authentication required'];
        }
        if (empty(array_intersect(['refresh_owned_ff', 'refresh_all_ff'], $_SESSION['permissions']))) {
            return ['http_error' => 403, 'reason' => 'No `'.implode('` or `', ['refresh_owned_ff', 'refresh_all_ff']).'` permission'];
        }
        $id_column = match ($this::ENTITY_TYPE) {
            'character' => 'character_id',
            'achievement' => 'achievement_id',
            'freecompany' => 'fc_id',
            'linkshell' => 'ls_id',
            'pvpteam' => 'pvp_id',
        };
        try {
            if ($this::ENTITY_TYPE !== 'achievement') {
                if ($this::ENTITY_TYPE === 'character') {
                    $check = Query::query('SELECT `character_id` FROM `uc__user_to_ff_character` WHERE `character_id` = :id AND `user_id`=:user_id', [':id' => $this->id, ':user_id' => $_SESSION['user_id']], return: 'check');
                    if (!$check) {
                        return ['http_error' => 403, 'reason' => 'Character not linked to user'];
                    }
                } else {
                    #Check if any character currently registered in a group is linked to the user
                    $check = Query::query('SELECT `'.$id_column.'` FROM `ffxiv__'.$this::ENTITY_TYPE.'_character` LEFT JOIN `uc__user_to_ff_character` ON `ffxiv__'.$this::ENTITY_TYPE.'_character`.`character_id`=`uc__user_to_ff_character`.`character_id` WHERE `'.$id_column.'` = :id AND `user_id`=:user_id', [':id' => $this->id, ':user_id' => $_SESSION['user_id']], return: 'check');
                    if (!$check) {
                        return ['http_error' => 403, 'reason' => 'Group not linked to user'];
                    }
                }
            }
            return $this->update();
        } catch (\Throwable $exception) {
            Errors::error_log($exception, debug: $this->debug);
            return ['http_error' => 503, 'reason' => 'Failed to validate linkage'];
        }
    }
    
    /**
     * Register the entity if it has not been registered already
     * @return bool|int
     */
    public function register(): bool|int
    {
        #Check if ID was set
        if ($this->id === null) {
            return 400;
        }
        $id_column = match ($this::ENTITY_TYPE) {
            'character' => 'character_id',
            'achievement' => 'achievement_id',
            'freecompany' => 'fc_id',
            'linkshell' => 'ls_id',
            'pvpteam' => 'pvp_id',
        };
        try {
            $check = Query::query('SELECT `'.$id_column.'` FROM `ffxiv__'.$this::ENTITY_TYPE.'` WHERE `'.$id_column.'` = :id', [':id' => $this->id], return: 'check');
        } catch (\Throwable $exception) {
            Errors::error_log($exception, debug: $this->debug);
            return 503;
        }
        if ($check) {
            #Entity already registered
            return 409;
        }
        #Try to get data from Lodestone
        try {
            $temp_lodestone = $this->getFromLodestone();
        } catch (\Throwable $exception) {
            Errors::error_log($exception, 'Failed to get '.$this::ENTITY_TYPE.' with ID '.$this->id, debug: $this->debug);
            return false;
        }
        if (!is_array($temp_lodestone)) {
            return 503;
        }
        $this->lodestone = $temp_lodestone;
        if (isset($this->lodestone['404']) && $this->lodestone['404'] === true) {
            return 404;
        }
        #Characters can mark their profiles as private on Lodestone since Dawntrail
        if ($this::ENTITY_TYPE === 'character' && isset($this->lodestone['private']) && $this->lodestone['private'] === true) {
            return 403;
        }
        #At some point, empty linkshells became possible on lodestone, those that have a page, but no members at all, and are not searchable by name. Possibly private linkshells or something like that
        #Since they lack some basic information, it's not possible to register them, so treat them as private
        if (isset($this->lodestone['empty']) && $this->lodestone['empty'] === true && in_array($this::ENTITY_TYPE, ['linkshell', 'crossworld_linkshell', 'crossworldlinkshell'])) {
            return 403;
        }
        unset($this->lodestone['404']);
        return $this->updateDB();
    }
    
    /**
     * Helper function to add new characters to Cron en masse
     * @param array $members
     *
     * @return void
     */
    protected function charMassCron(array $members): void
    {
        #Cache CRON object
        if (!empty($members)) {
            $cron = new TaskInstance();
            foreach ($members as $member => $details) {
                if (!$details['registered']) {
                    #Priority is higher since they are missing a lot of data.
                    try {
                        $cron->settingsFromArray(['task' => 'ff_update_entity', 'arguments' => [(string)$member, 'character'], 'message' => 'Updating character with ID '.$member, 'priority' => 2])->add();
                    } catch (\Throwable) {
                        #Do nothing, not considered critical
                    }
                }
            }
        }
    }
    
    /**
     * Function to remove Lodestone domain(s) from image links
     * @param string $url
     *
     * @return string
     */
    public static function removeLodestoneDomain(string $url): string
    {
        return str_replace([
            'https://img.finalfantasyxiv.com/lds/pc/global/images/itemicon/',
            'https://lds-img.finalfantasyxiv.com/itemicon/'
        ], '', $url);
    }
    
    /**
     * Function to download crest components from Lodestone
     * @param array $images
     *
     * @return void
     */
    public function downloadCrestComponents(array $images): void
    {
        foreach ($images as $key => $image) {
            if (!empty($image)) {
                #Emblem S7f_4f44211af230eac35370ef3e9fe15e51_07_128x128.png is not working, so it should be S7f_4f44211af230eac35370ef3e9fe15e51_08_128x128.png
                #This was fixed by SE at some point, but now it's broken again, so we change the URL ourselves
                $url_to_download = preg_replace('/S7f_4f44211af230eac35370ef3e9fe15e51_07_128x128.png/', 'S7f_4f44211af230eac35370ef3e9fe15e51_08_128x128.png', $image);
                #Check if we have already downloaded the component image and use that one to speed up the process
                if ($key === 0) {
                    #If it's background, we need to check if a subdirectory exists and create it, and create it if it does not
                    $sub_dir = mb_substr(basename($image), 0, 3, 'UTF-8');
                    $concurrent_directory = Config::$crests_components.'backgrounds/'.$sub_dir;
                    if (!is_dir($concurrent_directory) && !mkdir($concurrent_directory) && !is_dir($concurrent_directory)) {
                        throw new \RuntimeException(sprintf('Directory "%s" was not created', $concurrent_directory));
                    }
                } elseif ($key === 2) {
                    #If it's an emblem, we need to check if a subdirectory exists and create it, and create it if it does not
                    $sub_dir = mb_substr(basename($image), 0, 3, 'UTF-8');
                    $concurrent_directory = Config::$crests_components.'emblems/'.$sub_dir;
                    if (!is_dir($concurrent_directory) && !mkdir($concurrent_directory) && !is_dir($concurrent_directory)) {
                        throw new \RuntimeException(sprintf('Directory "%s" was not created', $concurrent_directory));
                    }
                } else {
                    $sub_dir = '';
                }
                $cached_image = self::crestToLocal($image);
                if (!empty($cached_image)) {
                    #Try downloading the component if it's not present locally
                    if (!is_file($cached_image)) {
                        Images::download($url_to_download, $cached_image, false);
                    }
                    #If it's an emblem, check that other emblem variants are downloaded as well
                    if ($key === 2) {
                        $emblem_index = (int)preg_replace('/(.+_)(\d{2})(_.+\.png)/', '$2', basename($image));
                        for ($iteration = 0; $iteration <= 7; $iteration++) {
                            if ($iteration !== $emblem_index) {
                                $emblem_file = Config::$crests_components.'emblems/'.$sub_dir.'/'.preg_replace('/(.+_)(\d{2})(_.+\.png)/', '${1}0'.$iteration.'$3', basename($image));
                                if (!is_file($emblem_file)) {
                                    #We generate the link to download an emblem
                                    #In addition S7f_4f44211af230eac35370ef3e9fe15e51_07_128x128.png is not working, so it should be S7f_4f44211af230eac35370ef3e9fe15e51_08_128x128.png
                                    #This was fixed by SE at some point, but now it's broken again, so we change the URL ourselves
                                    $url_to_download = preg_replace(['/(.+_)(\d{2})(_.+\.png)/', '/S7f_4f44211af230eac35370ef3e9fe15e51_07_128x128.png/'], ['${1}0'.$iteration.'$3', 'S7f_4f44211af230eac35370ef3e9fe15e51_08_128x128.png'], $image);
                                    try {
                                        Images::download($url_to_download, $emblem_file, false);
                                    } catch (\Throwable) {
                                        #Do nothing, not critical
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    /**
     * Function to turn a group crest into a favicon
     * @param array $images
     *
     * @return string|null
     */
    public static function crestToFavicon(array $images): ?string
    {
        $images = self::sortComponents($images);
        $merged_filenames = (empty($images[0]) ? '' : basename($images[0])).(empty($images[1]) ? '' : basename($images[1])).(empty($images[2]) ? '' : basename($images[2]));
        #Get hash of the merged images based on their names
        $crest_hash = hash('sha3-512', $merged_filenames);
        if (!empty($crest_hash)) {
            #Get a full path
            $full_path = mb_substr($crest_hash, 0, 2, 'UTF-8').'/'.mb_substr($crest_hash, 2, 2, 'UTF-8').'/'.$crest_hash.'.webp';
            #Generate an image file, if missing
            if (!is_file(Config::$merged_crests_cache.$full_path)) {
                self::crestMerge($images, Config::$merged_crests_cache.$full_path);
            }
            return '/assets/images/fftracker/merged-crests/'.$full_path;
        }
        return '/assets/images/fftracker/merged-crests/not_found.webp';
    }
    
    /**
     * Function converts image URL to a local path
     * @param string $image
     *
     * @return string|null
     */
    protected static function crestToLocal(string $image): ?string
    {
        $filename = basename($image);
        #Backgrounds
        if (str_starts_with($filename, 'F00') || str_starts_with($filename, 'B')) {
            return Config::$crests_components.'backgrounds/'.mb_substr($filename, 0, 3, 'UTF-8').'/'.$filename;
        }
        #Frames
        if (str_starts_with($filename, 'F')) {
            return Config::$crests_components.'frames/'.$filename;
        }
        #Emblems
        if (str_starts_with($filename, 'S')) {
            return Config::$crests_components.'emblems/'.mb_substr($filename, 0, 3, 'UTF-8').'/'.$filename;
        }
        Errors::error_log(new \UnexpectedValueException('Unexpected crest component URL `'.$image.'`'));
        return null;
    }
    
    /**
     * Sort crest components
     * @param array $images
     *
     * @return array
     */
    protected static function sortComponents(array $images): array
    {
        $images_to_merge = [];
        foreach ($images as $image) {
            if (!empty($image)) {
                $cached_image = self::crestToLocal($image);
                if ($cached_image !== null) {
                    if (str_contains($cached_image, 'backgrounds')) {
                        $images_to_merge[0] = $cached_image;
                    } elseif (str_contains($cached_image, 'frames')) {
                        $images_to_merge[1] = $cached_image;
                    } elseif (str_contains($cached_image, 'emblems')) {
                        $images_to_merge[2] = $cached_image;
                    }
                }
            }
        }
        ksort($images_to_merge);
        return $images_to_merge;
    }
    
    /**
     * Function to merge 1 to 3 images making up a crest on Lodestone into 1 stored on the tracker side
     *
     * @param array  $images     Array of crest components
     * @param string $final_path Where to save the final file
     * @param bool   $debug      Debug mode to log errors
     *
     * @return bool
     **/
    protected static function crestMerge(array $images, string $final_path, bool $debug = false): bool
    {
        try {
            #Don't do anything if an empty array
            if (empty($images)) {
                return false;
            }
            #Check if the path exists and create it recursively, if not
            /* @noinspection PhpUsageOfSilenceOperatorInspection */
            if (!is_dir(dirname($final_path)) && !@mkdir(dirname($final_path), recursive: true) && !is_dir(dirname($final_path))) {
                throw new \RuntimeException(sprintf('Directory "%s" was not created', $final_path));
            }
            $gd = Images::merge($images);
            #Save the file
            return $gd !== null && imagewebp($gd, $final_path, IMG_WEBP_LOSSLESS);
        } catch (\Throwable $exception) {
            if ($debug) {
                Errors::error_log($exception, debug: $debug);
            }
            return false;
        }
    }
    
    /**
     * Clean component crests to have a proper image, even if they are empty
     * @param array $results
     *
     * @return array
     */
    public static function cleanCrestResults(array $results): array
    {
        foreach ($results as $key => $result) {
            if (isset($result['crest_part_1']) || isset($result['crest_part_2']) || isset($result['crest_part_3'])) {
                $results[$key]['icon'] = self::crestToFavicon([$result['crest_part_1'], $result['crest_part_2'], $result['crest_part_3']]);
                if (isset($result['gc_id']) && str_contains($results[$key]['icon'], 'not_found') && in_array($result['gc_id'], [1, 2, 3], true)) {
                    $results[$key]['icon'] = $result['gc_id'];
                }
            } else {
                $results[$key]['icon'] = '/assets/images/fftracker/merged-crests/not_found.webp';
            }
            unset($results[$key]['crest_part_1'], $results[$key]['crest_part_2'], $results[$key]['crest_part_3'], $results[$key]['gc_id']);
        }
        return $results;
    }
}