package goll

import (
	"strings"
	"time"
)

type loadLimiterSingleTenantProxy struct {
	proxied   *loadLimiterDefaultImpl
	tenantKey string
}

type compositeLoadLimiterSingleTenantProxy struct {
	proxied   *compositeLoadLimiterDefaultImpl
	tenantKey string
}

var (
	singleTenantDefaultKey = "$"
)

func (instance *loadLimiterDefaultImpl) ForTenant(tenantKey string) SingleTenantStandaloneLoadLimiter {
	if strings.TrimSpace(tenantKey) == "" {
		panic("tenant key must not be blank")
	}
	if tenantKey == singleTenantDefaultKey {
		panic("tenant key must not be the reserved identifier: " + singleTenantDefaultKey)
	}
	proxy := loadLimiterSingleTenantProxy{
		proxied:   instance,
		tenantKey: tenantKey,
	}
	return &proxy
}

func (instance *loadLimiterDefaultImpl) AsSingleTenant() SingleTenantStandaloneLoadLimiter {
	proxy := loadLimiterSingleTenantProxy{
		proxied:   instance,
		tenantKey: singleTenantDefaultKey,
	}
	return &proxy
}

func (instance *loadLimiterSingleTenantProxy) Probe(load uint64) (bool, error) {
	return instance.proxied.Probe(instance.tenantKey, load)
}

func (instance *loadLimiterSingleTenantProxy) Submit(load uint64) (SubmitResult, error) {
	return instance.proxied.Submit(instance.tenantKey, load)
}

func (instance *loadLimiterSingleTenantProxy) SubmitUntil(load uint64, timeout time.Duration) error {
	return instance.proxied.SubmitUntil(instance.tenantKey, load, timeout)
}

func (instance *loadLimiterSingleTenantProxy) SubmitUntilWithDetails(load uint64, timeout time.Duration) SubmitUntilResult {
	return instance.proxied.SubmitUntilWithDetails(instance.tenantKey, load, timeout)
}

func (instance *loadLimiterSingleTenantProxy) Stats() (RuntimeStatistics, error) {
	return instance.proxied.Stats(instance.tenantKey)
}

func (instance *loadLimiterSingleTenantProxy) IsComposite() bool {
	return instance.proxied.IsComposite()
}

func (instance *compositeLoadLimiterDefaultImpl) ForTenant(tenantKey string) SingleTenantCompositeLoadLimiter {
	if strings.TrimSpace(tenantKey) == "" {
		panic("tenant key must not be blank")
	}
	if tenantKey == singleTenantDefaultKey {
		panic("tenant key must not be the reserved identifier: " + singleTenantDefaultKey)
	}
	proxy := compositeLoadLimiterSingleTenantProxy{
		proxied:   instance,
		tenantKey: tenantKey,
	}
	return &proxy
}

func (instance *compositeLoadLimiterDefaultImpl) AsSingleTenant() SingleTenantCompositeLoadLimiter {
	proxy := compositeLoadLimiterSingleTenantProxy{
		proxied:   instance,
		tenantKey: singleTenantDefaultKey,
	}
	return &proxy
}

func (instance *compositeLoadLimiterSingleTenantProxy) Probe(load uint64) (bool, error) {
	return instance.proxied.Probe(instance.tenantKey, load)
}

func (instance *compositeLoadLimiterSingleTenantProxy) Submit(load uint64) (SubmitResult, error) {
	return instance.proxied.Submit(instance.tenantKey, load)
}

func (instance *compositeLoadLimiterSingleTenantProxy) SubmitUntil(load uint64, timeout time.Duration) error {
	return instance.proxied.SubmitUntil(instance.tenantKey, load, timeout)
}

func (instance *compositeLoadLimiterSingleTenantProxy) SubmitUntilWithDetails(load uint64, timeout time.Duration) SubmitUntilResult {
	return instance.proxied.SubmitUntilWithDetails(instance.tenantKey, load, timeout)
}

func (instance *compositeLoadLimiterSingleTenantProxy) Stats() (CompositeRuntimeStatistics, error) {
	return instance.proxied.Stats(instance.tenantKey)
}

func (instance *compositeLoadLimiterSingleTenantProxy) IsComposite() bool {
	return instance.proxied.IsComposite()
}
