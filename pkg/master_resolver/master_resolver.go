package masterresolver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/flant/redis-sentinel-proxy/pkg/utils"
)

type RedisMasterResolver struct {
	masterName               string
	sentinelAddr             *net.TCPAddr
	sentinelPassword         string
	retryOnMasterResolveFail int

	masterAddrLock           *sync.RWMutex
	initialMasterResolveLock chan struct{}

	masterAddr string
}

func NewRedisMasterResolver(masterName string, sentinelAddr *net.TCPAddr, sentinelPassword string, retryOnMasterResolveFail int) *RedisMasterResolver {
	return &RedisMasterResolver{
		masterName:               masterName,
		sentinelAddr:             sentinelAddr,
		sentinelPassword:         sentinelPassword,
		retryOnMasterResolveFail: retryOnMasterResolveFail,
		masterAddrLock:           &sync.RWMutex{},
		initialMasterResolveLock: make(chan struct{}),
	}
}

func (r *RedisMasterResolver) MasterAddress() string {
	<-r.initialMasterResolveLock

	r.masterAddrLock.RLock()
	defer r.masterAddrLock.RUnlock()
	return r.masterAddr
}

func (r *RedisMasterResolver) setMasterAddress(masterAddr *net.TCPAddr) {
	r.masterAddrLock.Lock()
	defer r.masterAddrLock.Unlock()
	r.masterAddr = masterAddr.String()
}

func (r *RedisMasterResolver) updateMasterAddress() error {
	masterAddr, err := redisMasterFromSentinelAddr(r.sentinelAddr, r.sentinelPassword, r.masterName)
	if err != nil {
		log.Println(err)
		return err
	}
	r.setMasterAddress(masterAddr)
	return nil
}

func (r *RedisMasterResolver) UpdateMasterAddressLoop(ctx context.Context) error {
	if err := r.initialMasterAddressResolve(); err != nil {
		return err
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var err error
	for errCount := 0; errCount <= r.retryOnMasterResolveFail; {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		err = r.updateMasterAddress()
		if err != nil {
			errCount++
		} else {
			errCount = 0
		}
	}
	return err
}

func (r *RedisMasterResolver) initialMasterAddressResolve() error {
	defer close(r.initialMasterResolveLock)
	return r.updateMasterAddress()
}

func redisMasterFromSentinelAddr(sentinelAddress *net.TCPAddr, sentinelPassword string, masterName string) (*net.TCPAddr, error) {
	conn, err := utils.TCPConnectWithTimeout(sentinelAddress.String())
	if err != nil {
		return nil, fmt.Errorf("error connecting to sentinel: %w", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(time.Second))

	// Authenticate with sentinel if password is provided
	if sentinelPassword != "" {
		authCommand := fmt.Sprintf("AUTH %s\r\n", sentinelPassword)
		if _, err := conn.Write([]byte(authCommand)); err != nil {
			return nil, fmt.Errorf("error sending AUTH to sentinel: %w", err)
		}

		// Read response from AUTH
		b := make([]byte, 256)
		n, err := conn.Read(b)
		if err != nil {
			return nil, fmt.Errorf("error reading AUTH response: %w", err)
		}
		response := string(b[:n])
		if !strings.HasPrefix(response, "+OK") {
			return nil, fmt.Errorf("sentinel AUTH failed: %s", response)
		}
	}

	// Request master address
	getMasterCommand := fmt.Sprintf("SENTINEL get-master-addr-by-name %s\r\n", masterName)
	if _, err := conn.Write([]byte(getMasterCommand)); err != nil {
		return nil, fmt.Errorf("error writing to sentinel: %w", err)
	}

	// Read response
	b := make([]byte, 256)
	n, err := conn.Read(b)
	if err != nil {
		return nil, fmt.Errorf("error getting info from sentinel: %w", err)
	}

	// Extract master address parts
	parts := strings.Split(string(b[:n]), "\r\n")
	if len(parts) < 5 {
		return nil, errors.New("couldn't get master address from sentinel")
	}

	// Assemble master address
	formattedMasterAddress := fmt.Sprintf("%s:%s", parts[2], parts[4])
	addr, err := net.ResolveTCPAddr("tcp", formattedMasterAddress)
	if err != nil {
		return nil, fmt.Errorf("error resolving redis master: %w", err)
	}

	// Check if there is a Redis instance listening on the master address
	if err := checkTCPConnect(addr); err != nil {
		return nil, fmt.Errorf("error checking redis master: %w", err)
	}

	return addr, nil
}

func checkTCPConnect(addr *net.TCPAddr) error {
	conn, err := utils.TCPConnectWithTimeout(addr.String())
	if err != nil {
		return err
	}
	defer conn.Close()
	return nil
}
