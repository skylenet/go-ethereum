// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package adapters

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/pkg/jsonmessage"
	"github.com/docker/docker/pkg/reexec"
	"github.com/docker/go-connections/nat"
	"golang.org/x/net/websocket"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/rpc"

	p2pnat "github.com/ethereum/go-ethereum/p2p/nat"
)

const (
	// dockerImageName is the name of the docker image which gets built
	dockerImageName = "p2p-docker"

	dockerP2PPort        = 3333
	dockerWebsocketPort  = 4444
	dockerManagementPort = 5555
)

func init() {
	// Register a reexec function to start a simulation node when the current binary is
	// executed as "p2p-docker" (rather than whatever the main() function would normally do).
	reexec.Register("p2p-docker", execDockerNode)
}

// ManagementService is a service that is used to manage the DockerNodes
type ManagementService struct {
	waitConfig sync.WaitGroup
	Config     DockerNodeConfig
}

// Configure is used by the simulation framework to send the p2p
// configuration to the remote docker nodes.
func (svc *ManagementService) Configure(config DockerNodeConfig) error {
	svc.Config = config
	svc.waitConfig.Done()
	return nil
}

func (svc *ManagementService) waitForConfig() {
	svc.waitConfig.Add(1)
	svc.waitConfig.Wait()
}

type simpleWSDialer struct{}

// DialRPC implements the RPCDialer interface and return a Websocket RPC client
func (s *simpleWSDialer) DialRPC(id enode.ID) (*rpc.Client, error) {
	return rpc.DialWebsocket(context.Background(), id.String(), "http://localhost")
}

// execDockerNode is the main() function used by the docker nodes
func execDockerNode() {

	// log configuration
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlTrace, log.StreamHandler(os.Stderr, log.TerminalFormat(false))))
	log.PrintOrigins(true)
	log.Info("starting management server")

	// start a management websocket server to deal with incoming commands
	mgmtaddr := fmt.Sprintf("0.0.0.0:%d", dockerManagementPort)
	mgmtService := &ManagementService{}
	mgmtApis := []rpc.API{
		{
			Namespace: "mgmt",
			Version:   "1.0",
			Service:   mgmtService,
			Public:    true,
		},
	}

	l, _, err := rpc.StartWSEndpoint(mgmtaddr, mgmtApis, []string{}, []string{"*"}, true)
	if err != nil {
		log.Crit("failed to start management server", "err", err)
	}
	defer l.Close()

	// Wait for configuration
	// TODO: Use channels + timeout?

	log.Info("waiting for configuration")
	mgmtService.waitForConfig()
	log.Info("got configuration")

	natif, err := p2pnat.Parse(fmt.Sprintf("ip:%s", mgmtService.Config.IPAddress))
	if err != nil {
		log.Crit("failed to parse network ip address %s: %v", mgmtService.Config.IPAddress, err)
	}

	// reconstrut NodeConfig
	key, err := hex.DecodeString(mgmtService.Config.PrivateKey)
	if err != nil {
		log.Crit("could not decode private key", "err", err)
	}
	privKey, err := crypto.ToECDSA(key)
	if err != nil {
		log.Crit("could not create private key", "err", err)
	}

	nodeConfig := &NodeConfig{
		ID:              mgmtService.Config.ID,
		PrivateKey:      privKey,
		EnableMsgEvents: mgmtService.Config.EnableMsgEvents,
		Services:        mgmtService.Config.Services,
		Name:            mgmtService.Config.Name,
		DataDir:         "/data",
		Port:            dockerP2PPort,
	}

	// create actual node config
	c := node.DefaultConfig
	c.WSHost = "0.0.0.0"
	c.WSPort = dockerWebsocketPort
	c.WSOrigins = []string{"*"}
	c.WSExposeAll = true
	c.P2P.EnableMsgEvents = nodeConfig.EnableMsgEvents
	c.P2P.NoDiscovery = true
	c.P2P.NAT = natif
	c.NoUSB = true
	c.P2P.ListenAddr = fmt.Sprintf(":%d", nodeConfig.Port)
	c.DataDir = nodeConfig.DataDir
	c.P2P.Logger = log.New("node.id", mgmtService.Config.ID)
	c.P2P.PrivateKey = privKey

	log.Info("config ready")

	// initialize the devp2p node
	n, err := node.New(&c)
	if err != nil {
		log.Crit("failed to create devp2p node", "err", err)
	}

	rpcDialer := &simpleWSDialer{}
	services := make(map[string]node.Service, len(nodeConfig.Services))

	// register requested services
	for _, name := range nodeConfig.Services {
		name := name // pin variable to scope
		serviceFunc, exists := serviceFuncs[name]
		if !exists {
			log.Crit("unknown node service %q", err)
		}
		constructor := func(nodeCtx *node.ServiceContext) (node.Service, error) {
			ctx := &ServiceContext{
				RPCDialer:   rpcDialer,
				NodeContext: nodeCtx,
				Config:      nodeConfig,
			}
			if mgmtService.Config.Snapshots != nil {
				ctx.Snapshot = mgmtService.Config.Snapshots[name]
			}
			service, err := serviceFunc(ctx)
			if err != nil {
				return nil, err
			}
			services[name] = service
			return service, nil
		}
		if err := n.Register(constructor); err != nil {
			log.Crit(fmt.Sprintf("error registering service %q", name), "err", err)
		}
	}

	// register snapshot service
	err = n.Register(func(ctx *node.ServiceContext) (node.Service, error) {
		return &snapshotService{services}, nil
	})
	if err != nil {
		log.Crit("error starting snapshot service", "err", err)
	}

	// start p2p node
	if err = n.Start(); err != nil {
		log.Crit("failed to start devp2p node", "err", err)
	}

	// stop the node if we get a SIGTERM signal.
	go func() {
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, syscall.SIGTERM)
		defer signal.Stop(sigc)
		<-sigc
		log.Info("Received SIGTERM, shutting down...")
		if err := n.Stop(); err != nil {
			log.Error("Error stopping node", "err", err)
		}
	}()

	n.Wait() // Wait for the node to exit.
}

// DockerNodeConfig is used to define the p2p node configuration
// within a DockerNode.
type DockerNodeConfig struct {
	ID              enode.ID          `json:"id"`
	Name            string            `json:"name"`
	IPAddress       string            `json:"ip_addr"`
	PrivateKey      string            `json:"private_key"`
	Services        []string          `json:"services"`
	Snapshots       map[string][]byte `json:"snapshots"`
	EnableMsgEvents bool              `json:"enable_msg_events"`
}

// NewDockerNodeConfig creates a DockerNodeConfig based on a NodeConfig
func NewDockerNodeConfig(config *NodeConfig) DockerNodeConfig {
	return DockerNodeConfig{
		ID:              config.ID,
		Name:            config.Name,
		Services:        config.Services,
		EnableMsgEvents: config.EnableMsgEvents,
		PrivateKey:      hex.EncodeToString(crypto.FromECDSA(config.PrivateKey)),
	}
}

// DockerNode implements the Node interface and is used by the DockerAdapter
type DockerNode struct {
	DockerNodeConfig

	mgmtPort nat.PortBinding
	wsPort   nat.PortBinding
	p2pPort  nat.PortBinding

	wsClient   *rpc.Client
	mgmtClient *rpc.Client

	adapter *DockerAdapter
}

func (n *DockerNode) containerName() string {
	return fmt.Sprintf("%s-%s", dockerImageName, n.DockerNodeConfig.ID.TerminalString())
}

// Addr returns the node's address (e.g. an Enode URL)
func (n *DockerNode) Addr() []byte {
	return []byte(n.NodeInfo().Enode)
}

// Client returns the RPC client which is created once the node is
// up and running
func (n *DockerNode) Client() (*rpc.Client, error) {
	if n.wsClient == nil {
		// connect to devp2p rpc interface
		wsHost := fmt.Sprintf("ws://localhost:%s", n.wsPort.HostPort)
		err := retryBackoff(10, 200*time.Millisecond, func() error {
			var err error
			n.wsClient, err = rpc.DialWebsocket(context.Background(), wsHost, "http://localhost")
			if err != nil {
				log.Warn("error connecting to websocket rpc endpoint", "id", n.DockerNodeConfig.ID, "err", err)
			} else {
				log.Info("websocket rpc client created", "id", n.DockerNodeConfig.ID, "host", wsHost)
			}
			return err
		})
		if err != nil {
			return nil, err
		}
	}

	return n.wsClient, nil
}

// ServeRPC serves RPC requests over the given connection
func (n *DockerNode) ServeRPC(clientConn net.Conn) error {
	wsHost := fmt.Sprintf("ws://localhost:%s", n.wsPort.HostPort)
	conn, err := websocket.Dial(wsHost, "", "http://localhost")
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(2)
	join := func(src, dst net.Conn) {
		defer wg.Done()

		if _, err := io.Copy(dst, src); err != nil && err != io.EOF {
			log.Error("Failed serverpc copy", "dst", dst, "src", src, "err", err)
		}

		// close the write end of the destination connection
		if cw, ok := dst.(interface {
			CloseWrite() error
		}); ok {
			if err := cw.CloseWrite(); err != nil {
				log.Error("Failed closing destination connection", "dst", dst, "err", err)
			}
		} else {
			dst.Close()
		}
	}
	go join(conn, clientConn)
	go join(clientConn, conn)
	wg.Wait()
	return nil
}

// Start starts the node with the given snapshots
func (n *DockerNode) Start(snapshots map[string][]byte) (err error) {
	// TODO: check if node is already running?
	defer func() {
		if err != nil {
			log.Error("Stopping node due to errors", "err", err)
			if err := n.Stop(); err != nil {
				log.Error("Failed stopping node", "err", err)
			}
		}
	}()

	n.DockerNodeConfig.Snapshots = snapshots

	// start the node via a container
	ctx := context.Background()
	dockercli := n.adapter.client

	resp, err := dockercli.ContainerCreate(ctx, &container.Config{
		Image: dockerImageName,
		Cmd:   []string{"p2p-docker"},
		ExposedPorts: nat.PortSet{
			nat.Port(strconv.Itoa(dockerManagementPort)): struct{}{},
			nat.Port(strconv.Itoa(dockerP2PPort)):        struct{}{},
			nat.Port(strconv.Itoa(dockerWebsocketPort)):  struct{}{},
		},
	}, &container.HostConfig{
		PortBindings: map[nat.Port][]nat.PortBinding{
			nat.Port(strconv.Itoa(dockerManagementPort)): {{HostIP: "127.0.0.1", HostPort: "0"}},
			nat.Port(strconv.Itoa(dockerP2PPort)):        {{HostIP: "127.0.0.1", HostPort: "0"}},
			nat.Port(strconv.Itoa(dockerWebsocketPort)):  {{HostIP: "127.0.0.1", HostPort: "0"}},
		},
	}, nil, n.containerName())
	if err != nil {
		return fmt.Errorf("failed to create container %s: %v", n.containerName(), err)
	}

	if err := dockercli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
		return fmt.Errorf("failed to start container %s: %v", n.containerName(), err)
	}

	log.Info("Started container", "name", n.containerName())

	// get network ports
	cinfo := types.ContainerJSON{}
	err = retryBackoff(10, 500*time.Millisecond, func() error {
		var err error
		cinfo, err = dockercli.ContainerInspect(ctx, n.containerName())
		return err
	})
	if err != nil {
		log.Crit("Could not get container info", "err", err)
	}

	// get the container IP addr
	n.DockerNodeConfig.IPAddress = cinfo.NetworkSettings.IPAddress

	if val, ok := cinfo.NetworkSettings.Ports[nat.Port(fmt.Sprintf("%d/tcp", dockerManagementPort))]; ok {
		n.mgmtPort = val[0]
	} else {
		return fmt.Errorf("could not get management port for %s", n.containerName())
	}

	if val, ok := cinfo.NetworkSettings.Ports[nat.Port(fmt.Sprintf("%d/tcp", dockerP2PPort))]; ok {
		n.p2pPort = val[0]
	} else {
		return fmt.Errorf("could not get p2p port for %s", n.containerName())
	}

	if val, ok := cinfo.NetworkSettings.Ports[nat.Port(fmt.Sprintf("%d/tcp", dockerWebsocketPort))]; ok {
		n.wsPort = val[0]
	} else {
		return fmt.Errorf("could not get websocket port for %s", n.containerName())
	}

	// connect to management rpc interface
	mgmtHost := fmt.Sprintf("ws://localhost:%s", n.mgmtPort.HostPort)
	err = retryBackoff(10, 500*time.Millisecond, func() error {
		n.mgmtClient, err = rpc.DialWebsocket(context.Background(), mgmtHost, "http://localhost")
		if err != nil {
			log.Warn("Error connecting to management rpc endpoint", "id", n.DockerNodeConfig.ID, "err", err)
		}
		return err
	})
	if err != nil {
		log.Crit("Could not connect to management rpc endpoint", "err", err)
	}
	log.Info("Connected to management rpc endpoint", "id", n.DockerNodeConfig.ID, "host", mgmtHost)

	// send configuration to node
	if err := n.mgmtClient.Call(nil, "mgmt_configure", n.DockerNodeConfig); err != nil {
		log.Error("Error sending node configuration", "err", err)
		return err
	}

	// get container logs
	logOpts := types.ContainerLogsOptions{
		ShowStderr: true,
		ShowStdout: true,
		Follow:     true,
	}
	go func() {
		r, err := dockercli.ContainerLogs(context.Background(), n.containerName(), logOpts)
		if err != nil && err != io.EOF {
			log.Error("Error getting container logs", "err", err)
		}
		defer r.Close()

		if _, err := io.Copy(os.Stdout, r); err != nil && err != io.EOF {
			log.Error("Error writing container logs", "err", err)
		}
	}()

	// TODO: wait for node to be ready?
	return nil
}

// Stop stops the node
func (n *DockerNode) Stop() error {
	dockercli := n.adapter.client
	// TODO: check first if its running ?

	var stopTimeout = 30 * time.Second
	err := dockercli.ContainerStop(context.Background(), n.containerName(), &stopTimeout)
	if err != nil {
		return fmt.Errorf("failed to stop container %s : %v", n.containerName(), err)
	}

	err = dockercli.ContainerRemove(context.Background(), n.containerName(), types.ContainerRemoveOptions{})
	if err != nil {
		return fmt.Errorf("failed to remove container %s : %v", n.containerName(), err)
	}

	if n.wsClient != nil {
		n.wsClient.Close()
		n.wsClient = nil
	}
	if n.mgmtClient != nil {
		n.mgmtClient.Close()
		n.mgmtClient = nil
	}

	return nil
}

// NodeInfo returns information about the node
func (n *DockerNode) NodeInfo() *p2p.NodeInfo {
	info := &p2p.NodeInfo{
		ID: n.DockerNodeConfig.ID.String(),
	}
	if n.wsClient != nil {
		if err := n.wsClient.Call(&info, "admin_nodeInfo"); err != nil {
			log.Error("Failed calling admin_nodeInfo", "id", info.ID, "err", err)
		}
	}
	return info
}

// Snapshots creates snapshots of the running services
func (n *DockerNode) Snapshots() (map[string][]byte, error) {
	if n.wsClient == nil {
		return nil, errors.New("RPC not started")
	}
	var snapshots map[string][]byte
	return snapshots, n.wsClient.Call(&snapshots, "simulation_snapshot")
}

// DockerAdapter is a NodeAdapter which runs simulation on local docker containers
type DockerAdapter struct {
	client *client.Client
}

// NewDockerAdapter return a DockerAdapter.
func NewDockerAdapter() (*DockerAdapter, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, err
	}

	a := &DockerAdapter{
		client: cli,
	}

	err = a.buildImage()
	if err != nil {
		return nil, err
	}

	return a, nil
}

// Name returns the name of the adapter
func (a *DockerAdapter) Name() string {
	return "docker-adapter"
}

// NewNode returns a Node given the configuration
func (a *DockerAdapter) NewNode(config *NodeConfig) (Node, error) {
	// Validate that services exist and are registered upfront
	if len(config.Services) == 0 {
		return nil, errors.New("node must have at least one service")
	}

	for _, service := range config.Services {
		if _, exists := serviceFuncs[service]; !exists {
			return nil, fmt.Errorf("unknown node service %q", service)
		}
	}

	err := config.initDummyEnode()
	if err != nil {
		return nil, err
	}

	n := &DockerNode{
		adapter:          a,
		DockerNodeConfig: NewDockerNodeConfig(config),
	}

	return n, nil
}

// buildImage builds the Docker image which is used to run the simulation
// node in a Docker container.
//
// The binary is named as "p2p-docker" so that the execDockerNode function
// is executed as the main function.
func (a *DockerAdapter) buildImage() error {
	// create a directory to use as the build context
	dir, err := ioutil.TempDir("", "p2p-docker")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	// copy the current binary into the build context
	bin, err := os.Open(reexec.Self())
	if err != nil {
		return err
	}
	defer bin.Close()
	dst, err := os.OpenFile(filepath.Join(dir, "self.bin"), os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer dst.Close()
	if _, err := io.Copy(dst, bin); err != nil {
		return err
	}

	// create the Dockerfile
	dockerfile := []byte(`
FROM ubuntu:18.04
RUN mkdir /data
ADD self.bin /bin/p2p-docker
	`)
	if err := ioutil.WriteFile(filepath.Join(dir, "Dockerfile"), dockerfile, 0644); err != nil {
		return err
	}

	ctx, err := archive.TarWithOptions(dir, &archive.TarOptions{})
	if err != nil {
		return err
	}

	// build image
	opts := types.ImageBuildOptions{
		SuppressOutput: false,
		PullParent:     true,
		Tags:           []string{dockerImageName},
		Dockerfile:     "Dockerfile",
	}

	buildResp, err := a.client.ImageBuild(context.Background(), ctx, opts)
	if err != nil {
		return err
	}

	// parse build output
	d := json.NewDecoder(buildResp.Body)
	var event *jsonmessage.JSONMessage
	for {
		if err := d.Decode(&event); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		log.Info("Docker build", "msg", event.Stream)
		if event.Error != nil {
			log.Crit("Docker build error", "err", event.Error.Message)
		}
	}
	return nil
}

func retryBackoff(attempts int, minSleep time.Duration, f func() error) (err error) {
	for i := 0; ; i++ {
		err = f()
		if err == nil {
			return
		}
		if i >= (attempts - 1) {
			break
		}
		s := minSleep * time.Duration((i + 1))
		time.Sleep(s)
		log.Trace(fmt.Sprintf("Retrying in %s after error:", s), "err", err)
	}
	return fmt.Errorf("failed after %d attempts, last error: %s", attempts, err)
}
