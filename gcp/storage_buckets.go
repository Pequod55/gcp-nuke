package gcp

import (
        "fmt"
        "log"
        "sync"
        "time"

        "github.com/pequod55/gcp-nuke/config"
        "github.com/pequod55/gcp-nuke/helpers"
        "golang.org/x/sync/errgroup"
        "golang.org/x/sync/syncmap"
        "google.golang.org/api/storage/v1"
)

// StorageBuckets -
type StorageBuckets struct {
        serviceClient *storage.Service
        base          ResourceBase
        resourceMap   syncmap.Map
}

func init() {
        storageService, err := storage.NewService(Ctx)
        if err != nil {
                log.Fatal(err)
        }
        storageResource := StorageBuckets{
                serviceClient: storageService,
        }
        register(&storageResource)
}

// Name - Name of the resourceLister for StorageBuckets
func (c *StorageBuckets) Name() string {
        return "StorageBuckets"
}

// ToSlice - Name of the resourceLister for StorageBuckets
func (c *StorageBuckets) ToSlice() (slice []string) {
        return helpers.SortedSyncMapKeys(&c.resourceMap)

}

// Setup - populates the struct
func (c *StorageBuckets) Setup(config config.Config) {
        c.base.config = config
}

// List - Returns a list of all StorageBuckets
func (c *StorageBuckets) List(refreshCache bool) []string {
        if !refreshCache {
                return c.ToSlice()
        }
        // Refresh resource map
        c.resourceMap = sync.Map{}

        bucketListCall := c.serviceClient.Buckets.List(c.base.config.Project)
        bucketList, err := bucketListCall.Do()
        if err != nil {
                log.Fatal(err)
        }

        for _, bucket := range bucketList.Items {
                c.resourceMap.Store(bucket.Name, nil)
        }
        return c.ToSlice()
}

// Dependencies - Returns a List of resource names to check for
func (c *StorageBuckets) Dependencies() []string {
        return []string{}
}

// Remove -
func (c *StorageBuckets) Remove() error {

        // Removal logic
        errs, _ := errgroup.WithContext(c.base.config.Context)

        c.resourceMap.Range(func(key, value interface{}) bool {
                bucketID := key.(string)

                // Parallel bucket deletion
                errs.Go(func() error {
                        deleteCall := c.serviceClient.Buckets.Delete(bucketID)
                        err := deleteCall.Do()
                        if err != nil {
                                return err
                        }
                        seconds := 0
                        for {
                                log.Printf("[Info] Resource currently being deleted %v [type: %v project: %v] (%v seconds)", bucketID, c.Name(), c.base.config.Project, seconds)

                                getCall := c.serviceClient.Buckets.Get(bucketID)
                                _, err := getCall.Do()
                                if err != nil {
                                        c.resourceMap.Delete(bucketID)

                                        log.Printf("[Info] Resource deleted %v [type: %v project: %v] (%v seconds)", bucketID, c.Name(), c.base.config.Project, seconds)
                                        return nil
                                }

                                time.Sleep(time.Duration(c.base.config.PollTime) * time.Second)
                                seconds += c.base.config.PollTime
                                if seconds > c.base.config.Timeout {
                                        return fmt.Errorf("[Error] Resource deletion timed out for %v [type: %v project: %v] (%v seconds)", bucketID, c.Name(), c.base.config.Project, c.base.config.Timeout)
                                }
                        }
                })
                return true
        })
        // Wait for all deletions to complete, and return the first non nil error
        err := errs.Wait()
        return err
}
