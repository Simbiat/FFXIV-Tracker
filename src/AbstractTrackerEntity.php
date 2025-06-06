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
    protected string $idFormat = '/^\d+$/m';
    #Debug flag
    protected bool $debug = false;
    protected const entityType = 'character';
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
        if (preg_match($this->idFormat, $id) !== 1) {
            throw new \UnexpectedValueException('ID `'.$id.'` for entity `'.\get_class($this).'` has incorrect format.');
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
        } catch (\Throwable $e) {
            $error = $e->getMessage().$e->getTraceAsString();
            Errors::error_log($e);
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
     * @param bool $allowSleep Whether to wait in case Lodestone throttles the request (that is throttle on our side)
     *
     * @return string|array
     */
    abstract public function getFromLodestone(bool $allowSleep = false): string|array;
    
    /**
     * Function to do processing
     * @param array $fromDB
     *
     * @return void
     */
    abstract protected function process(array $fromDB): void;
    
    /**
     * Function to update the entity in DB
     * @return bool
     */
    abstract protected function updateDB(): bool;
    
    /**
     * Update the entity
     * @param bool $allowSleep Flag to allow sleep, in case Lodestone is throttling us
     *
     * @return string|bool
     */
    public function update(bool $allowSleep = false): string|bool
    {
        #Check if ID was set
        if ($this->id === null) {
            return false;
        }
        #Check if we have not updated before
        try {
            $updated = Query::query('SELECT `updated` FROM `ffxiv__'.$this::entityType.'` WHERE `'.$this::entityType.'id` = :id', [':id' => $this->id], return: 'value');
        } catch (\Throwable $e) {
            Errors::error_log($e, debug: $this->debug);
            return $e->getMessage()."\n".$e->getTraceAsString();
        }
        #Check if it has not been updated recently (10 minutes, to protect from potential abuse)
        if (isset($updated) && (time() - strtotime($updated)) < 600) {
            #Return entity type
            return true;
        }
        #Try to get data from Lodestone, if not already taken
        if (!is_array($this->lodestone)) {
            try {
                $tempLodestone = $this->getFromLodestone($allowSleep);
            } catch (\Throwable $exception) {
                Errors::error_log($exception, 'Failed to get '.$this::entityType.' with ID '.$this->id, debug: $this->debug);
                return $exception->getMessage()."\r\n".$exception->getTraceAsString();
            }
            if (!is_array($tempLodestone)) {
                return $tempLodestone;
            }
            $this->lodestone = $tempLodestone;
        }
        #If we got 404, return true. If an entity is to be removed, it's done during getFromLodestone()
        if (isset($this->lodestone['404']) && $this->lodestone['404'] === true) {
            return true;
        }
        #Characters can mark their profiles as private on Lodestone since Dawntrail
        if ($this::entityType === 'character' && isset($this->lodestone['private']) && $this->lodestone['private'] === true) {
            return true;
        }
        unset($this->lodestone['404']);
        if (empty($this->lodestone['name'])) {
            return 'No name found for '.$this::entityType.' ID `'.$this->id.'`';
        }
        return $this->updateDB();
    }
    
    /**
     * To be called from API to allow entity updates
     * @return bool|array|string
     */
    public function updateFromApi(): bool|array|string
    {
        if ($_SESSION['userid'] === 1) {
            return ['http_error' => 403, 'reason' => 'Authentication required'];
        }
        if (empty(array_intersect(['refreshOwnedFF', 'refreshAllFF'], $_SESSION['permissions']))) {
            return ['http_error' => 403, 'reason' => 'No `'.implode('` or `', ['refreshOwnedFF', 'refreshAllFF']).'` permission'];
        }
        try {
            if ($this::entityType !== 'achievement') {
                if ($this::entityType === 'character') {
                    $check = Query::query('SELECT `characterid` FROM `uc__user_to_ff_character` WHERE `characterid` = :id AND `userid`=:userid', [':id' => $this->id, ':userid' => $_SESSION['userid']], return: 'check');
                    if (!$check) {
                        return ['http_error' => 403, 'reason' => 'Character not linked to user'];
                    }
                } else {
                    #Check if any character currently registered in a group is linked to the user
                    $check = Query::query('SELECT `'.$this::entityType.'id` FROM `ffxiv__'.$this::entityType.'_character` LEFT JOIN `uc__user_to_ff_character` ON `ffxiv__'.$this::entityType.'_character`.`characterid`=`uc__user_to_ff_character`.`characterid` WHERE `'.$this::entityType.'id` = :id AND `userid`=:userid', [':id' => $this->id, ':userid' => $_SESSION['userid']], return: 'check');
                    if (!$check) {
                        return ['http_error' => 403, 'reason' => 'Group not linked to user'];
                    }
                }
            }
            return $this->update();
        } catch (\Throwable $e) {
            Errors::error_log($e, debug: $this->debug);
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
        try {
            $check = Query::query('SELECT `'.$this::entityType.'id` FROM `ffxiv__'.$this::entityType.'` WHERE `'.$this::entityType.'id` = :id', [':id' => $this->id], return: 'check');
        } catch (\Throwable $e) {
            Errors::error_log($e, debug: $this->debug);
            return 503;
        }
        if ($check) {
            #Entity already registered
            return 409;
        }
        #Try to get data from Lodestone
        try {
            $tempLodestone = $this->getFromLodestone();
        } catch (\Throwable $exception) {
            Errors::error_log($exception, 'Failed to get '.$this::entityType.' with ID '.$this->id, debug: $this->debug);
            return false;
        }
        if (!is_array($tempLodestone)) {
            return 503;
        }
        $this->lodestone = $tempLodestone;
        if (isset($this->lodestone['404']) && $this->lodestone['404'] === true) {
            return 404;
        }
        #Characters can mark their profiles as private on Lodestone since Dawntrail
        if ($this::entityType === 'character' && isset($this->lodestone['private']) && $this->lodestone['private'] === true) {
            return 403;
        }
        #At some point empty linkshells became possible on lodestone, those that have a page, but no members at all, and are not searchable by name. Possibly private linkshells or something like that
        #Since they lack some basic information, it's not possible to register them, so treat them as private
        if (isset($this->lodestone['empty']) && $this->lodestone['empty'] === true && \in_array($this::entityType, ['linkshell', 'crossworld_linkshell', 'crossworldlinkshell'])) {
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
                        $cron->settingsFromArray(['task' => 'ffUpdateEntity', 'arguments' => [(string)$member, 'character'], 'message' => 'Updating character with ID '.$member, 'priority' => 2])->add();
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
    protected function downloadCrestComponents(array $images): void
    {
        foreach ($images as $key => $image) {
            if (!empty($image)) {
                #Check if we have already downloaded the component image and use that one to speed up the process
                if ($key === 0) {
                    #If it's background, we need to check if a subdirectory exists and create it, and create it if it does not
                    $subDir = mb_strtolower(mb_substr(basename($image), 0, 3, 'UTF-8'), 'UTF-8');
                    $concurrentDirectory = Config::$crestsComponents.'backgrounds/'.$subDir;
                    if (!is_dir($concurrentDirectory) && !mkdir($concurrentDirectory) && !is_dir($concurrentDirectory)) {
                        throw new \RuntimeException(sprintf('Directory "%s" was not created', $concurrentDirectory));
                    }
                } elseif ($key === 2) {
                    #If it's an emblem, we need to check if a subdirectory exists and create it, and create it if it does not
                    $subDir = mb_strtolower(mb_substr(basename($image), 0, 3, 'UTF-8'), 'UTF-8');
                    $concurrentDirectory = Config::$crestsComponents.'emblems/'.$subDir;
                    if (!is_dir($concurrentDirectory) && !mkdir($concurrentDirectory) && !is_dir($concurrentDirectory)) {
                        throw new \RuntimeException(sprintf('Directory "%s" was not created', $concurrentDirectory));
                    }
                } else {
                    $subDir = '';
                }
                $cachedImage = self::crestToLocal($image);
                if (!empty($cachedImage)) {
                    #Try downloading the component if it's not present locally
                    if (!is_file($cachedImage)) {
                        Images::download($image, $cachedImage, false);
                    }
                    #If it's an emblem, check that other emblem variants are downloaded as well
                    if ($key === 2) {
                        $emblemIndex = (int)preg_replace('/(.+_)(\d{2})(_.+\.png)/', '$2', basename($image));
                        for ($i = 0; $i <= 7; $i++) {
                            if ($i !== $emblemIndex) {
                                $emblemFile = Config::$crestsComponents.'emblems/'.$subDir.'/'.preg_replace('/(.+_)(\d{2})(_.+\.png)/', '${1}0'.$i.'$3', basename($image));
                                if (!is_file($emblemFile)) {
                                    try {
                                        Images::download(preg_replace('/(.+_)(\d{2})(_.+\.png)/', '${1}0'.$i.'$3', $image), $emblemFile, false);
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
        $mergedFileNames = (empty($images[0]) ? '' : basename($images[0])).(empty($images[1]) ? '' : basename($images[1])).(empty($images[2]) ? '' : basename($images[2]));
        #Get hash of the merged images based on their names
        $crestHash = hash('sha3-512', $mergedFileNames);
        if (!empty($crestHash)) {
            #Get a full path
            $fullPath = mb_substr($crestHash, 0, 2, 'UTF-8').'/'.mb_substr($crestHash, 2, 2, 'UTF-8').'/'.$crestHash.'.webp';
            #Generate an image file, if missing
            if (!is_file(Config::$mergedCrestsCache.$fullPath)) {
                self::CrestMerge($images, Config::$mergedCrestsCache.$fullPath);
            }
            return '/assets/images/fftracker/merged-crests/'.$fullPath;
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
            return Config::$crestsComponents.'backgrounds/'.mb_strtolower(mb_substr($filename, 0, 3, 'UTF-8'), 'UTF-8').'/'.$filename;
        }
        #Frames
        if (str_starts_with($filename, 'F')) {
            return Config::$crestsComponents.'frames/'.$filename;
        }
        #Emblems
        if (str_starts_with($filename, 'S')) {
            return Config::$crestsComponents.'emblems/'.mb_strtolower(mb_substr($filename, 0, 3, 'UTF-8'), 'UTF-8').'/'.$filename;
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
        $imagesToMerge = [];
        foreach ($images as $image) {
            if (!empty($image)) {
                $cachedImage = self::crestToLocal($image);
                if ($cachedImage !== null) {
                    if (str_contains($cachedImage, 'backgrounds')) {
                        $imagesToMerge[0] = $cachedImage;
                    } elseif (str_contains($cachedImage, 'frames')) {
                        $imagesToMerge[1] = $cachedImage;
                    } elseif (str_contains($cachedImage, 'emblems')) {
                        $imagesToMerge[2] = $cachedImage;
                    }
                }
            }
        }
        ksort($imagesToMerge);
        return $imagesToMerge;
    }
    
    /**
     * Function to merge 1 to 3 images making up a crest on Lodestone into 1 stored on the tracker side
     *
     * @param array  $images    Array of crest components
     * @param string $finalPath Where to save the final file
     * @param bool   $debug     Debug mode to log errors
     *
     * @return bool
     **/
    protected static function CrestMerge(array $images, string $finalPath, bool $debug = false): bool
    {
        try {
            #Don't do anything if empty array
            if (empty($images)) {
                return false;
            }
            #Check if the path exists and create it recursively, if not
            /* @noinspection PhpUsageOfSilenceOperatorInspection */
            if (!is_dir(dirname($finalPath)) && !@mkdir(dirname($finalPath), recursive: true) && !is_dir(dirname($finalPath))) {
                throw new \RuntimeException(sprintf('Directory "%s" was not created', $finalPath));
            }
            $gd = Images::merge($images);
            #Save the file
            return $gd !== null && imagewebp($gd, $finalPath, IMG_WEBP_LOSSLESS);
        } catch (\Throwable $e) {
            if ($debug) {
                Errors::error_log($e, debug: $debug);
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
                if (isset($result['grandcompanyid']) && str_contains($results[$key]['icon'], 'not_found') && \in_array($result['grandcompanyid'], [1, 2, 3], true)) {
                    $results[$key]['icon'] = $result['grandcompanyid'];
                }
            } else {
                $results[$key]['icon'] = '/assets/images/fftracker/merged-crests/not_found.webp';
            }
            unset($results[$key]['crest_part_1'], $results[$key]['crest_part_2'], $results[$key]['crest_part_3'], $results[$key]['grandcompanyid']);
        }
        return $results;
    }
}