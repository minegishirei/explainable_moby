

# 読むべきソースコード

## Docker Deamonのソースコード

<a href="/minegishirei/explainable_moby/daemon/daemon.go">docker deamonのソースコード</a>

具体例：docker deamonシャットダウン時の処理

```go
// Shutdown stops the daemon.
func (daemon *Daemon) Shutdown(ctx context.Context) error {
	daemon.shutdown = true
	// Keep mounts and networking running on daemon shutdown if
	// we are to keep containers running and restore them.

	cfg := &daemon.config().Config
	if cfg.LiveRestoreEnabled && daemon.containers != nil {
		// check if there are any running containers, if none we should do some cleanup
		if ls, err := daemon.Containers(ctx, &types.ContainerListOptions{}); len(ls) != 0 || err != nil {
			// metrics plugins still need some cleanup
			daemon.cleanupMetricsPlugins()
			return err
		}
	}

	if daemon.containers != nil {
		logrus.Debugf("daemon configured with a %d seconds minimum shutdown timeout", cfg.ShutdownTimeout)
		logrus.Debugf("start clean shutdown of all containers with a %d seconds timeout...", daemon.shutdownTimeout(cfg))
		daemon.containers.ApplyAll(func(c *container.Container) {
			if !c.IsRunning() {
				return
			}
			log := logrus.WithField("container", c.ID)
			log.Debug("shutting down container")
			if err := daemon.shutdownContainer(c); err != nil {
				log.WithError(err).Error("failed to shut down container")
				return
			}
			if mountid, err := daemon.imageService.GetLayerMountID(c.ID); err == nil {
				daemon.cleanupMountsByID(mountid)
			}
			log.Debugf("shut down container")
		})
	}

	if daemon.volumes != nil {
		if err := daemon.volumes.Shutdown(); err != nil {
			logrus.Errorf("Error shutting down volume store: %v", err)
		}
	}

	if daemon.imageService != nil {
		if err := daemon.imageService.Cleanup(); err != nil {
			logrus.Error(err)
		}
	}

	// If we are part of a cluster, clean up cluster's stuff
	if daemon.clusterProvider != nil {
		logrus.Debugf("start clean shutdown of cluster resources...")
		daemon.DaemonLeavesCluster()
	}

	daemon.cleanupMetricsPlugins()

	// Shutdown plugins after containers and layerstore. Don't change the order.
	daemon.pluginShutdown()

	// trigger libnetwork Stop only if it's initialized
	if daemon.netController != nil {
		daemon.netController.Stop()
	}

	if daemon.containerdCli != nil {
		daemon.containerdCli.Close()
	}

	if daemon.mdDB != nil {
		daemon.mdDB.Close()
	}

	return daemon.cleanupMounts(cfg)
}
```


## layer.go

<a href="/minegishirei/explainable_moby/layer/layer.go">docker layerのソースコード</a>




dockerのイメージはレイヤーが重なることで出来上がる。
基本的なレイヤーは読み取り専用であり、そのレイヤーを重ねることで変更をする。

```go
// Layer represents a read-only layer
type Layer interface {
	TarStreamer

	// TarStreamFrom returns a tar archive stream for all the layer chain with
	// arbitrary depth.
	TarStreamFrom(ChainID) (io.ReadCloser, error)

	// ChainID returns the content hash of the entire layer chain. The hash
	// chain is made up of DiffID of top layer and all of its parents.
	ChainID() ChainID

	// DiffID returns the content hash of the layer
	// tar stream used to create this layer.
	DiffID() DiffID

	// Parent returns the next layer in the layer chain.
	Parent() Layer

	// Size returns the size of the entire layer chain. The size
	// is calculated from the total size of all files in the layers.
	Size() int64

	// DiffSize returns the size difference of the top layer
	// from parent layer.
	DiffSize() int64

	// Metadata returns the low level storage metadata associated
	// with layer.
	Metadata() (map[string]string, error)
}
```





## Dockerfile

<a src="minegishirei/explainable_moby/Dockerfile">Dockerfile</a>



## Swaggerfile

<a src="minegishirei/explainable_moby/api/swagger.yaml">swagger.yml</a>







