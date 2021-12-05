package goll

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testSyncAdapter struct {
	collector []string
	returning map[string]string

	LockMock        func(context.Context, string) error
	FetchStatusMock func(context.Context, string) (string, error)
	WriteStatusMock func(context.Context, string, string) error
	UnlockMock      func(context.Context, string) error
}

func (c *testSyncAdapter) Clear() {
	c.collector = make([]string, 0)
	c.returning = make(map[string]string)
	c.LockMock = nil
	c.FetchStatusMock = nil
	c.WriteStatusMock = nil
	c.UnlockMock = nil
}

func (c *testSyncAdapter) Lock(arg context.Context, tenantKey string) error {
	c.collector = append(c.collector, "LOCK "+tenantKey)
	if c.LockMock != nil {
		return c.LockMock(arg, tenantKey)
	}
	return nil
}

func (c *testSyncAdapter) Fetch(arg context.Context, tenantKey string) (string, error) {
	c.collector = append(c.collector, "FETCH "+tenantKey)
	if c.FetchStatusMock != nil {
		return c.FetchStatusMock(arg, tenantKey)
	}
	return c.returning[tenantKey], nil
}

func (c *testSyncAdapter) Write(arg context.Context, tenantKey string, s string) error {
	c.collector = append(c.collector, "WRITE "+tenantKey+" "+s)
	if c.WriteStatusMock != nil {
		return c.WriteStatusMock(arg, tenantKey, s)
	}
	c.returning[tenantKey] = s
	return nil
}

func (c *testSyncAdapter) Unlock(arg context.Context, tenantKey string) error {
	c.collector = append(c.collector, "UNLOCK "+tenantKey)
	if c.UnlockMock != nil {
		return c.UnlockMock(arg, tenantKey)
	}
	return nil
}

func TestSyncAdapter(t *testing.T) {
	// provide a mock adapter
	adapter := testSyncAdapter{}
	adapter.Clear()

	ci := buildInstance(t, func(c *Config) {
		c.SyncAdapter = &adapter
	})

	// on the first probe
	_, _ = ci.Instance.Probe(defaultTestTenantKey, 1)

	// check that the sync adapter was called
	assert.Equal(t, []string{
		"LOCK test",
		"FETCH test",
		"UNLOCK test",
	}, adapter.collector)

	adapter.Clear()
	_, _ = ci.Instance.Submit(defaultTestTenantKey, 5)

	ci.AssertWindowStatus(t, defaultTestTenantKey, 5, "1000000:5")
	assert.Equal(t, uint64(3), ci.Instance.getTenant(defaultTestTenantKey).Version)

	// check that the sync adapter was called
	assert.Equal(t, []string{
		"LOCK test",
		"FETCH test",
		"WRITE test v1/3/5/0/1000000:5",
		"UNLOCK test",
	}, adapter.collector)

	// simulate update from another client
	adapter.Clear()
	adapter.returning[defaultTestTenantKey] = "v1/4/15/0/1000000:15"

	// now probe to fetch the status
	_, _ = ci.Instance.Probe(defaultTestTenantKey, 1)

	// check that the sync adapter was called
	assert.Equal(t, []string{
		"LOCK test",
		"FETCH test",
		"UNLOCK test",
	}, adapter.collector)

	// check that the status is up to date
	ci.AssertWindowStatus(t, defaultTestTenantKey, 15, "1000000:15")
	assert.Equal(t, uint64(4), ci.Instance.getTenant(defaultTestTenantKey).Version)

	// simulate update from another client
	ci.TimeTravel(2000)
	adapter.Clear()
	adapter.returning[defaultTestTenantKey] = "v1/10/30/0/1002000:5,1001000:10,1000000:15"

	_, _ = ci.Instance.Submit(defaultTestTenantKey, 5)

	ci.AssertWindowStatus(t, defaultTestTenantKey, 35, "1002000:10, 1001000:10, 1000000:15")
	assert.Equal(t, uint64(11), ci.Instance.getTenant(defaultTestTenantKey).Version)

	// check that the sync adapter was called
	assert.Equal(t, []string{
		"LOCK test",
		"FETCH test",
		"WRITE test v1/11/35/0/1002000:10,1001000:10,1000000:15",
		"UNLOCK test",
	}, adapter.collector)
}

func TestSyncAdapterErrorOnLock(t *testing.T) {
	// provide a mock adapter
	adapter := testSyncAdapter{}
	adapter.Clear()

	ci := buildInstance(t, func(c *Config) {
		c.SyncAdapter = &adapter
	})

	// force error on Lock on the mock adapter
	adapter.LockMock = func(ctx context.Context, tenantKey string) error {
		return errors.New("I could not")
	}

	_, err := ci.Instance.Submit(defaultTestTenantKey, 1)

	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "I could not")

	// check that the sync adapter was called
	assert.Equal(t, []string{
		"LOCK test",
	}, adapter.collector)

	assert.Equal(t, uint64(1), ci.Instance.getTenant(defaultTestTenantKey).Version)
}

func TestSyncAdapterErrorOnUnlock(t *testing.T) {
	// provide a mock adapter
	adapter := testSyncAdapter{}
	adapter.Clear()

	ci := buildInstance(t, func(c *Config) {
		c.SyncAdapter = &adapter
	})

	// force error on Unlock on the mock adapter
	adapter.UnlockMock = func(sc context.Context, tk string) error {
		return errors.New("I could not")
	}

	_, err := ci.Instance.Submit(defaultTestTenantKey, 1)

	assert.Nil(t, err)

	// check that the sync adapter was called
	assert.Equal(t, []string{
		"LOCK test",
		"FETCH test",
		"WRITE test v1/3/1/0/1000000:1",
		"UNLOCK test",
	}, adapter.collector)

	assert.Equal(t, uint64(3), ci.Instance.getTenant(defaultTestTenantKey).Version)
}

func TestSyncAdapterErrorOnFetch(t *testing.T) {
	// provide a mock adapter
	adapter := testSyncAdapter{}
	adapter.Clear()

	ci := buildInstance(t, func(c *Config) {
		c.SyncAdapter = &adapter
	})

	// force error on Unlock on the mock adapter
	adapter.FetchStatusMock = func(sc context.Context, tk string) (string, error) {
		return "", errors.New("I could not")
	}

	_, err := ci.Instance.Submit(defaultTestTenantKey, 1)

	assert.Nil(t, err)

	// check that the sync adapter was called
	assert.Equal(t, []string{
		"LOCK test",
		"FETCH test",
		"WRITE test v1/3/1/0/1000000:1",
		"UNLOCK test",
	}, adapter.collector)

	assert.Equal(t, uint64(3), ci.Instance.getTenant(defaultTestTenantKey).Version)
}

func TestSyncAdapterErrorOnStatusRestore(t *testing.T) {
	// provide a mock adapter
	adapter := testSyncAdapter{}
	adapter.Clear()

	ci := buildInstance(t, func(c *Config) {
		c.SyncAdapter = &adapter
	})

	// force error on Unlock on the mock adapter
	adapter.returning[defaultTestTenantKey] = "v1/AAA/BBB"

	_, err := ci.Instance.Submit(defaultTestTenantKey, 1)

	assert.Nil(t, err)

	// check that the sync adapter was called
	assert.Equal(t, []string{
		"LOCK test",
		"FETCH test",
		"WRITE test v1/3/1/0/1000000:1",
		"UNLOCK test",
	}, adapter.collector)

	assert.Equal(t, uint64(3), ci.Instance.getTenant(defaultTestTenantKey).Version)
}

func TestSyncAdapterErrorOnStatusWrite(t *testing.T) {
	// provide a mock adapter
	adapter := testSyncAdapter{}
	adapter.Clear()

	ci := buildInstance(t, func(c *Config) {
		c.SyncAdapter = &adapter
	})

	// force error on Unlock on the mock adapter
	adapter.WriteStatusMock = func(sc context.Context, tk string, s string) error {
		return errors.New("I could not")
	}

	_, err := ci.Instance.Submit(defaultTestTenantKey, 1)

	assert.Nil(t, err)

	// check that the sync adapter was called
	assert.Equal(t, []string{
		"LOCK test",
		"FETCH test",
		"WRITE test v1/3/1/0/1000000:1",
		"UNLOCK test",
	}, adapter.collector)

	assert.Equal(t, uint64(3), ci.Instance.getTenant(defaultTestTenantKey).Version)
}

func TestSyncAdapterComposite(t *testing.T) {
	// provide a mock adapter
	adapter := testSyncAdapter{}
	adapter.Clear()

	ci := buildCompositeInstance(t, func(c *CompositeConfig) {
		c.SyncAdapter = &adapter
	})

	// on the first probe
	_, _ = ci.Instance.Probe(defaultTestTenantKey, 1)

	// check that the sync adapter was called
	assert.Equal(t, []string{
		"LOCK test",
		"FETCH test",
		"UNLOCK test",
	}, adapter.collector)

	adapter.Clear()
	_, _ = ci.Instance.Submit(defaultTestTenantKey, 5)

	ci.AssertWindowStatus(t, defaultTestTenantKey, []uint64{5, 5}, "0:1000000:5, 1:1000000:5")

	// check that the sync adapter was called
	assert.Equal(t, []string{
		"LOCK test",
		"FETCH test",
		"WRITE test v1/3/5/0/1000000:5;v1/3/5/0/1000000:5",
		"UNLOCK test",
	}, adapter.collector)

	// simulate update from another client
	adapter.Clear()
	adapter.returning[defaultTestTenantKey] = "v1/4/15/0/1000000:15;v1/4/15/0/1000000:15"

	// now probe to fetch the status
	_, _ = ci.Instance.Probe(defaultTestTenantKey, 1)

	// check that the sync adapter was called
	assert.Equal(t, []string{
		"LOCK test",
		"FETCH test",
		"UNLOCK test",
	}, adapter.collector)

	// check that the status is up to date
	ci.AssertWindowStatus(t, defaultTestTenantKey, []uint64{15, 15}, "0:1000000:15, 1:1000000:15")

	// simulate update from another client
	ci.TimeTravel(2000)
	adapter.Clear()
	adapter.returning[defaultTestTenantKey] = "v1/10/30/0/1002000:5,1001000:10,1000000:15;v1/10/6/0/1002000:1,1001900:2,1001800:3"

	_, _ = ci.Instance.Submit(defaultTestTenantKey, 5)

	ci.AssertWindowStatus(
		t,
		defaultTestTenantKey,
		[]uint64{35, 11},
		"0:1002000:10, 0:1001000:10, 0:1000000:15, 1:1002000:6, 1:1001900:2, 1:1001800:3",
	)

	// check that the sync adapter was called
	assert.Equal(t, []string{
		"LOCK test",
		"FETCH test",
		"WRITE test v1/11/35/0/1002000:10,1001000:10,1000000:15;v1/11/11/0/1002000:6,1001900:2,1001800:3",
		"UNLOCK test",
	}, adapter.collector)
}
