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
        "google.golang.org/api/sqladmin/v1beta4"
)

// SQLInstances -
type SQLInstances struct {
        serviceClient *sqladmin.Service
        base          ResourceBase
        resourceMap   syncmap.Map
}

func init() {
        sqlService, err := sqladmin.NewService(Ctx)
        if err != nil {
                log.Fatal(err)
        }
        sqlResource := SQLInstances{
                serviceClient: sqlService,
        }
        register(&sqlResource)
}

// Name - Name of the resourceLister for SQLInstances
func (c *SQLInstances) Name() string {
        return "SQLInstances"
}

// ToSlice - Name of the resourceLister for SQLInstances
func (c *SQLInstances) ToSlice() (slice []string) {
        return helpers.SortedSyncMapKeys(&c.resourceMap)

}

// Setup - populates the struct
func (c *SQLInstances) Setup(config config.Config) {
        c.base.config = config
}

// List - Returns a list of all SQLInstances
func (c *SQLInstances) List(refreshCache bool) []string {
        if !refreshCache {
                return c.ToSlice()
        }
        // Refresh resource map
        c.resourceMap = sync.Map{}

        instanceListCall := c.serviceClient.Instances.List(c.base.config.Project)
        instanceList, err := instanceListCall.Do()
        if err != nil {
                log.Fatal(err)
        }

        for _, instance := range instanceList.Items {
                instanceResource := DefaultResourceProperties{}
                c.resourceMap.Store(instance.Name, instanceResource)
        }
        return c.ToSlice()
}

// Dependencies - Returns a List of resource names to check for
func (c *SQLInstances) Dependencies() []string {
        return []string{}
}

// Remove -
func (c *SQLInstances) Remove() error {

        // Removal logic
        errs, _ := errgroup.WithContext(c.base.config.Context)

        c.resourceMap.Range(func(key, value interface{}) bool {
                instanceID := key.(string)

                // Parallel instance deletion
                errs.Go(func() error {
                        deleteCall := c.serviceClient.Instances.Delete(c.base.config.Project, instanceID)
                        operation, err := deleteCall.Do()
                        if err != nil {
                                return err
                        }
                        var opStatus string
                        seconds := 0
                        for opStatus != "DONE" {
                                log.Printf("[Info] Resource currently being deleted %v [type: %v project: %v] (%v seconds)", instanceID, c.Name(), c.base.config.Project, seconds)
                                operationCall := c.serviceClient.Operations.Get(c.base.config.Project, operation.Name)
                                checkOpp, err := operationCall.Do()
                                if err != nil {
                                        return err
                                }
                                opStatus = checkOpp.Status

                                time.Sleep(time.Duration(c.base.config.PollTime) * time.Second)
                                seconds += c.base.config.PollTime
                                if seconds > c.base.config.Timeout {
                                        return fmt.Errorf("[Error] Resource deletion timed out for %v [type: %v project: %v] (%v seconds)", instanceID, c.Name(), c.base.config.Project, c.base.config.Timeout)
                                }
                        }
                        c.resourceMap.Delete(instanceID)

                        log.Printf("[Info] Resource deleted %v [type: %v project: %v] (%v seconds)", instanceID, c.Name(), c.base.config.Project, seconds)
                        return nil
                })
                return true
        })
        // Wait for all deletions to complete, and return the first non nil error
        err := errs.Wait()
        return err
}